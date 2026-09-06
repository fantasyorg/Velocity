package com.velocitypowered.proxy.network.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The tunnel socket's handler: routes inbound frames to their streams and collects outbound frames
 * from every stream into the socket, which is flushed on a timer or when enough bytes are queued.
 *
 * <p>Runs on the socket's event loop. Streams live on their own loops, so a frame from a stream is
 * handed over with {@code lazyExecute}: no wakeup, the timer that flushes will run it. Inbound
 * frames are grouped per stream for one read cycle and delivered with a single hop per stream.
 *
 * <p>The side that opened the socket keeps it alive with PINGs; the other side answers. A socket
 * with no PONG for {@link TunnelProtocol#PING_TIMEOUT_SECONDS} is closed, which closes every stream.
 */
public abstract class TunnelMultiplexer extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LogManager.getLogger(TunnelMultiplexer.class);

    private final Map<Integer, TunnelChildChannel> streams = new HashMap<>();
    private final Map<TunnelChildChannel, List<ByteBuf>> readBatch = new LinkedHashMap<>();
    private final List<TunnelChildChannel> closedInBatch = new ArrayList<>();
    private final boolean initiator;
    private final int flushIntervalMillis;
    protected final int windowBytes;

    private ChannelHandlerContext ctx;
    private AbstractEventExecutor loop;
    private ScheduledFuture<?> flushTask;
    private ScheduledFuture<?> pingTask;
    private int pendingBytes;
    private boolean dirty;
    private long lastPongNanos;

    protected TunnelMultiplexer(boolean initiator, int flushIntervalMillis, int windowBytes) {
        this.initiator = initiator;
        this.flushIntervalMillis = flushIntervalMillis;
        this.windowBytes = windowBytes;
    }

    /** A stream the peer opened; only the backend side receives these. */
    protected abstract void onOpen(int streamId, InetSocketAddress remote);

    /** The socket closed with these streams still open. */
    protected void onSocketClosed() {
    }

    public ChannelHandlerContext context() {
        return this.ctx;
    }

    public int streamCount() {
        return this.streams.size();
    }

    // ---- outbound, callable from any thread ----

    public void send(TunnelFrame frame) {
        if (this.loop.inEventLoop()) {
            write0(frame);
        } else {
            this.loop.lazyExecute(() -> write0(frame));
        }
    }

    void streamClosed(int streamId) {
        this.loop.lazyExecute(() -> this.streams.remove(streamId));
    }

    protected void registerStream(TunnelChildChannel child) {
        this.streams.put(child.streamId(), child);
    }

    protected TunnelChildChannel stream(int streamId) {
        return this.streams.get(streamId);
    }

    // ---- socket lifecycle ----

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        this.loop = (AbstractEventExecutor) ctx.executor();
        this.lastPongNanos = System.nanoTime();
        if (ctx.channel().isActive()) {
            startTimers();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (this.flushTask == null) {
            startTimers();
        }
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof TunnelFrame frame)) {
            ctx.fireChannelRead(msg);
            return;
        }

        switch (frame.type()) {
            case TunnelProtocol.FRAME_DATA -> onData(frame);
            case TunnelProtocol.FRAME_OPEN -> onOpenFrame(frame);
            case TunnelProtocol.FRAME_CLOSE -> onClose(frame);
            case TunnelProtocol.FRAME_WINDOW -> onWindow(frame);
            case TunnelProtocol.FRAME_PING -> {
                send(TunnelFrame.pong(frame.content().retain()));
                frame.release();
            }
            case TunnelProtocol.FRAME_PONG -> {
                this.lastPongNanos = System.nanoTime();
                frame.release();
            }
            default -> {
                LOGGER.warn("Unknown tunnel frame type {} from {}", frame.type(), ctx.channel().remoteAddress());
                frame.release();
            }
        }
    }

    /** Data first, then closes, so a stream closed right after its last bytes still gets them. */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        for (Map.Entry<TunnelChildChannel, List<ByteBuf>> entry : this.readBatch.entrySet()) {
            TunnelChildChannel child = entry.getKey();
            List<ByteBuf> payloads = entry.getValue();
            if (child.loop().inEventLoop()) {
                child.deliver(payloads);
            } else {
                child.loop().execute(() -> child.deliver(payloads));
            }
        }
        this.readBatch.clear();

        for (TunnelChildChannel child : this.closedInBatch) {
            if (child.loop().inEventLoop()) {
                child.peerClosed();
            } else {
                child.loop().execute(child::peerClosed);
            }
        }
        this.closedInBatch.clear();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        stopTimers();

        List<TunnelChildChannel> open = new ArrayList<>(this.streams.values());
        this.streams.clear();
        for (TunnelChildChannel child : open) {
            child.loop().execute(child::peerClosed);
        }

        onSocketClosed();
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("Tunnel socket {} failed, closing it and its {} streams", ctx.channel().remoteAddress(), this.streams.size(), cause);
        ctx.close();
    }

    // ---- frames ----

    private void onData(TunnelFrame frame) {
        TunnelChildChannel child = this.streams.get(frame.streamId());
        if (child == null) {
            frame.release();
            return;
        }
        this.readBatch.computeIfAbsent(child, key -> new ArrayList<>()).add(frame.content());
    }

    private void onOpenFrame(TunnelFrame frame) {
        try {
            if (this.streams.containsKey(frame.streamId())) {
                LOGGER.warn("Tunnel {} reopened stream {}", this.ctx.channel().remoteAddress(), frame.streamId());
                send(TunnelFrame.close(frame.streamId()));
                return;
            }
            onOpen(frame.streamId(), TunnelProtocol.readAddress(frame.content()));
        } finally {
            frame.release();
        }
    }

    private void onClose(TunnelFrame frame) {
        TunnelChildChannel child = this.streams.remove(frame.streamId());
        frame.release();
        if (child != null) {
            this.closedInBatch.add(child);
        }
    }

    private void onWindow(TunnelFrame frame) {
        TunnelChildChannel child = this.streams.get(frame.streamId());
        int credits = frame.content().readableBytes() >= 4 ? frame.content().readInt() : 0;
        frame.release();
        if (child == null || credits <= 0) {
            return;
        }
        child.loop().execute(() -> child.addCredits(credits));
    }

    // ---- writing and timers ----

    private void write0(TunnelFrame frame) {
        if (!this.ctx.channel().isActive()) {
            frame.release();
            return;
        }
        this.pendingBytes += frame.wireLength();
        this.dirty = true;
        this.ctx.write(frame, this.ctx.voidPromise());
        if (this.pendingBytes >= TunnelProtocol.FLUSH_THRESHOLD_BYTES) {
            flushNow();
        }
    }

    private void flushNow() {
        this.pendingBytes = 0;
        this.dirty = false;
        this.ctx.flush();
    }

    private void startTimers() {
        this.flushTask = this.loop.scheduleAtFixedRate(() -> {
            if (this.dirty) {
                flushNow();
            }
        }, this.flushIntervalMillis, this.flushIntervalMillis, TimeUnit.MILLISECONDS);

        if (this.initiator) {
            this.pingTask = this.loop.scheduleAtFixedRate(() -> {
                if (System.nanoTime() - this.lastPongNanos > TimeUnit.SECONDS.toNanos(TunnelProtocol.PING_TIMEOUT_SECONDS)) {
                    LOGGER.warn("Tunnel {} stopped answering pings, closing it", this.ctx.channel().remoteAddress());
                    this.ctx.close();
                    return;
                }
                send(TunnelFrame.ping(this.ctx.alloc(), System.nanoTime()));
            }, TunnelProtocol.PING_INTERVAL_SECONDS, TunnelProtocol.PING_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    private void stopTimers() {
        if (this.flushTask != null) {
            this.flushTask.cancel(false);
            this.flushTask = null;
        }
        if (this.pingTask != null) {
            this.pingTask.cancel(false);
            this.pingTask = null;
        }
    }
}
