package com.velocitypowered.proxy.network.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

/**
 * Wire format of the proxy tunnel: one TCP connection between a proxy and a backend that carries
 * every player of that pair as a numbered stream. The proxy opens the connection, sends the
 * handshake, and from then on both sides exchange frames.
 *
 * <p>Handshake (proxy -> backend): {@code magic u32, version u8, secretLength u16, secret bytes}.
 * Reply (backend -> proxy): {@code magic u32, version u8, status u8} where status 0 accepts.
 *
 * <p>Frame: {@code type u8, streamId u32, payloadLength u32, payload}. The proxy owns stream ids;
 * a stream is a player's connection, and its payload bytes are the exact Minecraft protocol bytes
 * that would have gone through a socket of its own.
 */
public final class TunnelProtocol {
    public static final int MAGIC = 0x46545531;
    public static final byte VERSION = 1;
    public static final byte STATUS_OK = 0;
    public static final byte STATUS_REJECTED = 1;

    public static final int HEADER_LENGTH = 9;
    public static final int MAX_PAYLOAD_LENGTH = 8 * 1024 * 1024;
    public static final int MAX_SECRET_LENGTH = 1024;

    public static final byte FRAME_OPEN = 1;
    public static final byte FRAME_DATA = 2;
    public static final byte FRAME_CLOSE = 3;
    public static final byte FRAME_WINDOW = 4;
    public static final byte FRAME_PING = 5;
    public static final byte FRAME_PONG = 6;

    /** Bytes a receiver is willing to buffer per stream before the sender must wait for a WINDOW. */
    public static final int DEFAULT_WINDOW_BYTES = 1 << 20;
    /** How long frames may sit in the socket buffer before the timer flushes them. */
    public static final int DEFAULT_FLUSH_INTERVAL_MILLIS = 10;
    /** Queued bytes that flush the socket right away instead of waiting for the timer. */
    public static final int FLUSH_THRESHOLD_BYTES = 256 * 1024;
    public static final int PING_INTERVAL_SECONDS = 5;
    public static final int PING_TIMEOUT_SECONDS = 30;

    private TunnelProtocol() {
    }

    public static ByteBuf encodeHandshake(ByteBufAllocator allocator, byte[] secret) {
        ByteBuf buf = allocator.buffer(4 + 1 + 2 + secret.length);
        buf.writeInt(MAGIC);
        buf.writeByte(VERSION);
        buf.writeShort(secret.length);
        buf.writeBytes(secret);
        return buf;
    }

    public static ByteBuf encodeHandshakeReply(ByteBufAllocator allocator, byte status) {
        ByteBuf buf = allocator.buffer(6);
        buf.writeInt(MAGIC);
        buf.writeByte(VERSION);
        buf.writeByte(status);
        return buf;
    }

    public static byte[] secretBytes(String secret) {
        return secret.getBytes(StandardCharsets.UTF_8);
    }

    /** OPEN payload: the player's real address, so the backend sees it before any forwarding. */
    public static void writeAddress(ByteBuf buf, InetSocketAddress address) {
        byte[] raw = address.getAddress() == null ? new byte[0] : address.getAddress().getAddress();
        buf.writeByte(raw.length);
        buf.writeBytes(raw);
        buf.writeShort(address.getPort());
    }

    public static InetSocketAddress readAddress(ByteBuf buf) {
        int length = buf.readUnsignedByte();
        byte[] raw = new byte[length];
        buf.readBytes(raw);
        int port = buf.readUnsignedShort();
        if (length == 0) {
            return new InetSocketAddress(port);
        }
        try {
            return new InetSocketAddress(InetAddress.getByAddress(raw), port);
        } catch (UnknownHostException e) {
            return new InetSocketAddress(port);
        }
    }
}
