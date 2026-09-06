package com.velocitypowered.proxy.network.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.buffer.Unpooled;

import java.net.InetSocketAddress;

/** One tunnel frame; the payload is owned by the frame and released with it. */
public final class TunnelFrame extends DefaultByteBufHolder {
    private final byte type;
    private final int streamId;

    public TunnelFrame(byte type, int streamId, ByteBuf payload) {
        super(payload);
        this.type = type;
        this.streamId = streamId;
    }

    public static TunnelFrame open(ByteBufAllocator allocator, int streamId, InetSocketAddress remote) {
        ByteBuf payload = allocator.buffer(1 + 16 + 2);
        TunnelProtocol.writeAddress(payload, remote);
        return new TunnelFrame(TunnelProtocol.FRAME_OPEN, streamId, payload);
    }

    public static TunnelFrame data(int streamId, ByteBuf payload) {
        return new TunnelFrame(TunnelProtocol.FRAME_DATA, streamId, payload);
    }

    public static TunnelFrame close(int streamId) {
        return new TunnelFrame(TunnelProtocol.FRAME_CLOSE, streamId, Unpooled.EMPTY_BUFFER);
    }

    public static TunnelFrame window(ByteBufAllocator allocator, int streamId, int credits) {
        return new TunnelFrame(TunnelProtocol.FRAME_WINDOW, streamId, allocator.buffer(4).writeInt(credits));
    }

    public static TunnelFrame ping(ByteBufAllocator allocator, long nanos) {
        return new TunnelFrame(TunnelProtocol.FRAME_PING, 0, allocator.buffer(8).writeLong(nanos));
    }

    public static TunnelFrame pong(ByteBuf payload) {
        return new TunnelFrame(TunnelProtocol.FRAME_PONG, 0, payload);
    }

    public byte type() {
        return this.type;
    }

    public int streamId() {
        return this.streamId;
    }

    /** Bytes this frame occupies on the wire. */
    public int wireLength() {
        return TunnelProtocol.HEADER_LENGTH + content().readableBytes();
    }

    @Override
    public TunnelFrame replace(ByteBuf content) {
        return new TunnelFrame(this.type, this.streamId, content);
    }

    @Override
    public TunnelFrame retain() {
        super.retain();
        return this;
    }

    @Override
    public TunnelFrame touch(Object hint) {
        super.touch(hint);
        return this;
    }

    @Override
    public String toString() {
        return "TunnelFrame{type=" + this.type + ", stream=" + this.streamId + ", bytes=" + content().readableBytes() + '}';
    }
}
