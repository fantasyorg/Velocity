package com.velocitypowered.proxy.network.tunnel;

import com.velocitypowered.proxy.VelocityServer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The proxy's tunnel to one backend: a single socket, opened on the first stream request and
 * reopened on the next request after it drops. Streams asked for while the socket is still
 * connecting wait for it; if it fails, they fail the way a refused socket would.
 */
public final class BackendTunnel {

  private static final Logger logger = LogManager.getLogger(BackendTunnel.class);
  /** Direct-connect window after a failed attempt; doubles per consecutive failure up to the cap. */
  private static final long UNAVAILABLE_SECONDS = 30;
  private static final long UNAVAILABLE_SECONDS_MAX = 300;

  private final VelocityServer server;
  private final String serverName;
  private final InetSocketAddress backendAddress;
  private final InetSocketAddress tunnelAddress;

  private CompletableFuture<TunnelClientMultiplexer> connecting;
  private volatile long unavailableUntilNanos;
  private volatile long unavailableSeconds = UNAVAILABLE_SECONDS;

  BackendTunnel(VelocityServer server, String serverName, InetSocketAddress backendAddress, InetSocketAddress tunnelAddress) {
    this.server = server;
    this.serverName = serverName;
    this.backendAddress = backendAddress;
    this.tunnelAddress = tunnelAddress;
  }

  public InetSocketAddress getTunnelAddress() {
    return this.tunnelAddress;
  }

  public InetSocketAddress getBackendAddress() {
    return this.backendAddress;
  }

  /** Drops the socket, if any; open streams end the way they do when a backend goes away. */
  public synchronized void close() {
    CompletableFuture<TunnelClientMultiplexer> current = this.connecting;
    this.connecting = null;
    if (current == null) {
      return;
    }
    current.thenAccept(mux -> mux.context().close());
  }

  /**
   * Whether the last attempt to reach the tunnel failed recently. Callers connect the ordinary way
   * meanwhile instead of paying a failed handshake per player.
   */
  public boolean isUnavailable() {
    return System.nanoTime() - this.unavailableUntilNanos < 0;
  }

  /**
   * Opens a stream for a player. Completes on {@code loop} with the stream channel once it is
   * registered and initialised, or fails if the tunnel cannot be established.
   */
  public CompletableFuture<Channel> openStream(EventLoop loop, InetSocketAddress playerAddress, ChannelHandler initializer) {
    CompletableFuture<Channel> stream = new CompletableFuture<>();

    multiplexer().whenComplete((mux, error) -> {
      if (error != null) {
        // Marked before the caller sees the failure, so its fallback decision reads the flag.
        markUnavailable();
        stream.completeExceptionally(error);
        return;
      }
      mux.openStream(loop, playerAddress, initializer).addListener((ChannelFutureListener) future -> {
        if (future.isSuccess()) {
          stream.complete(future.channel());
        } else {
          stream.completeExceptionally(future.cause());
        }
      });
    });

    return stream;
  }

  private synchronized CompletableFuture<TunnelClientMultiplexer> multiplexer() {
    if (this.connecting != null) {
      TunnelClientMultiplexer current = this.connecting.getNow(null);
      boolean alive = current == null ? !this.connecting.isCompletedExceptionally() : current.context().channel().isActive();
      if (alive) {
        return this.connecting;
      }
    }

    CompletableFuture<TunnelClientMultiplexer> future = new CompletableFuture<>();
    this.connecting = future;
    connect(future);
    return future;
  }

  private void connect(CompletableFuture<TunnelClientMultiplexer> future) {
    byte[] secret = this.server.getConfiguration().getForwardingSecret();
    int flushInterval = this.server.getConfiguration().getBackendTunnelFlushIntervalMillis();
    int window = this.server.getConfiguration().getBackendTunnelWindowBytes();

    logger.info("Opening tunnel to {} at {}", this.serverName, this.tunnelAddress);

    this.server.createBootstrap(null)
        .handler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel channel) {
            channel.pipeline().addLast("tunnel-handshake", new TunnelClientHandshakeHandler(secret,
                () -> new TunnelClientMultiplexer(BackendTunnel.this.backendAddress, flushInterval, window, BackendTunnel.this::closed), future));
          }
        })
        .connect(this.tunnelAddress)
        .addListener(connectFuture -> {
          if (!connectFuture.isSuccess()) {
            future.completeExceptionally(connectFuture.cause());
          }
        });

    future.whenComplete((mux, error) -> {
      if (error != null) {
        long directSeconds = markUnavailable();
        logger.warn("Tunnel to {} at {} failed, connecting directly for the next {}s: {}", this.serverName, this.tunnelAddress, directSeconds, error.toString());
        synchronized (this) {
          if (this.connecting == future) {
            this.connecting = null;
          }
        }
      } else {
        this.unavailableSeconds = UNAVAILABLE_SECONDS;
        logger.info("Tunnel to {} established", this.serverName);
      }
    });
  }

  /**
   * Starts the direct-connect window for this failure, or returns the one already running when the
   * sibling callback of the same failure got here first. Each consecutive failure doubles it.
   */
  private synchronized long markUnavailable() {
    long now = System.nanoTime();
    long remaining = this.unavailableUntilNanos - now;
    if (remaining > 0) {
      return TimeUnit.NANOSECONDS.toSeconds(remaining);
    }
    long window = this.unavailableSeconds;
    this.unavailableUntilNanos = now + TimeUnit.SECONDS.toNanos(window);
    this.unavailableSeconds = Math.min(UNAVAILABLE_SECONDS_MAX, window * 2);
    return window;
  }

  private synchronized void closed() {
    logger.warn("Tunnel to {} closed; the next connection reopens it", this.serverName);
    this.connecting = null;
  }
}
