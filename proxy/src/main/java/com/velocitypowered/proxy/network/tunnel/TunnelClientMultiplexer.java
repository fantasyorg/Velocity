package com.velocitypowered.proxy.network.tunnel;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proxy side of a tunnel socket: opens streams for players. A stream is registered on the player's
 * own event loop, so the backend connection keeps running where Velocity expects it to.
 */
public final class TunnelClientMultiplexer extends TunnelMultiplexer {

  private final InetSocketAddress backendAddress;
  private final AtomicInteger nextStreamId = new AtomicInteger(1);
  private final Runnable onClosed;

  public TunnelClientMultiplexer(InetSocketAddress backendAddress, int flushIntervalMillis, int windowBytes, Runnable onClosed) {
    super(true, flushIntervalMillis, windowBytes);
    this.backendAddress = backendAddress;
    this.onClosed = onClosed;
  }

  /**
   * Opens a stream to the backend for a player. The returned future completes when the stream
   * channel is registered on {@code loop} and initialised by {@code initializer}; it fails if the
   * socket is gone.
   */
  public ChannelFuture openStream(EventLoop loop, InetSocketAddress playerAddress, ChannelHandler initializer) {
    int streamId = this.nextStreamId.getAndIncrement();
    TunnelChildChannel child = new TunnelChildChannel(context().channel(), this, streamId, this.backendAddress, playerAddress, loop, this.windowBytes);
    child.pipeline().addLast(initializer);
    ChannelPromise promise = new DefaultChannelPromise(child, loop);

    context().executor().execute(() -> {
      if (!context().channel().isActive()) {
        promise.setFailure(new ConnectException("Tunnel to " + this.backendAddress + " is closed"));
        return;
      }
      registerStream(child);
      send(TunnelFrame.open(context().alloc(), streamId, playerAddress));
      loop.register(promise);
    });

    return promise;
  }

  @Override
  protected void onOpen(int streamId, InetSocketAddress remote) {
    send(TunnelFrame.close(streamId));
  }

  @Override
  protected void onSocketClosed() {
    this.onClosed.run();
  }
}
