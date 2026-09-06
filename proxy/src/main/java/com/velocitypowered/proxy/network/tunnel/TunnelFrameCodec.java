package com.velocitypowered.proxy.network.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

/** Frame header codec for the tunnel socket; the payload is passed through as a slice. */
public final class TunnelFrameCodec {

    private TunnelFrameCodec() {
    }

    public static final class Decoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            while (in.readableBytes() >= TunnelProtocol.HEADER_LENGTH) {
                int start = in.readerIndex();
                byte type = in.getByte(start);
                int streamId = in.getInt(start + 1);
                int length = in.getInt(start + 5);

                if (length < 0 || length > TunnelProtocol.MAX_PAYLOAD_LENGTH) {
                    throw new CorruptedFrameException("Tunnel frame payload of " + length + " bytes on stream " + streamId);
                }

                if (in.readableBytes() < TunnelProtocol.HEADER_LENGTH + length) {
                    return;
                }

                in.skipBytes(TunnelProtocol.HEADER_LENGTH);
                out.add(new TunnelFrame(type, streamId, in.readRetainedSlice(length)));
            }
        }
    }

    public static final class Encoder extends MessageToByteEncoder<TunnelFrame> {
        @Override
        protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, TunnelFrame frame, boolean preferDirect) {
            return ctx.alloc().directBuffer(frame.wireLength());
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, TunnelFrame frame, ByteBuf out) {
            ByteBuf payload = frame.content();
            out.writeByte(frame.type());
            out.writeInt(frame.streamId());
            out.writeInt(payload.readableBytes());
            out.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
        }
    }
}
