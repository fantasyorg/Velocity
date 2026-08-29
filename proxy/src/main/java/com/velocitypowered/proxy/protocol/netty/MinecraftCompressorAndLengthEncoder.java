/*
 * Copyright (C) 2018-2023 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.protocol.netty;

import static com.velocitypowered.proxy.protocol.netty.MinecraftVarintLengthEncoder.IS_JAVA_CIPHER;

import com.velocitypowered.natives.compression.VelocityCompressor;
import com.velocitypowered.natives.util.MoreByteBufUtils;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.util.zip.DataFormatException;

/**
 * Handler for compressing Minecraft packets.
 *
 * <p>Besides raw payloads, accepts a {@link CompressedFrame} taken from a backend connection: when
 * it was compressed under the same threshold as this connection it is written as it is (with one
 * length varint in front); otherwise it is inflated and compressed like any other payload.
 */
public class MinecraftCompressorAndLengthEncoder extends MessageToByteEncoder<Object> {

  private int threshold;
  private final VelocityCompressor compressor;

  public MinecraftCompressorAndLengthEncoder(int threshold, VelocityCompressor compressor) {
    this.threshold = threshold;
    this.compressor = compressor;
  }

  @Override
  public boolean acceptOutboundMessage(Object msg) {
    return msg instanceof ByteBuf || msg instanceof CompressedFrame;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object message, ByteBuf out) throws Exception {
    if (message instanceof CompressedFrame frame) {
      if (canForward(frame)) {
        ByteBuf content = frame.content();
        int length = content.readableBytes();
        ProtocolUtils.writeVarInt(out, length);
        out.writeBytes(content, content.readerIndex(), length);
        CompressedFrameStats.passed(length);
        return;
      }
      ByteBuf inflated = inflate(ctx, frame);
      try {
        CompressedFrameStats.recompressed(inflated.readableBytes());
        encodePayload(ctx, inflated, out);
      } finally {
        inflated.release();
      }
      return;
    }
    encodePayload(ctx, (ByteBuf) message, out);
  }

  private boolean canForward(CompressedFrame frame) {
    return !frame.isForceRecompress() && frame.getThreshold() == this.threshold;
  }

  /** Recovers the raw payload of a frame that cannot travel compressed as it is. */
  private ByteBuf inflate(ChannelHandlerContext ctx, CompressedFrame frame)
      throws DataFormatException {
    ByteBuf content = frame.content().slice();
    ProtocolUtils.readVarInt(content); // the uncompressed size, already known from the frame
    ByteBuf compatibleIn = MoreByteBufUtils.ensureCompatible(ctx.alloc(), compressor, content);
    ByteBuf uncompressed = MoreByteBufUtils.preferredBuffer(ctx.alloc(), compressor,
        frame.getUncompressedSize());
    try {
      compressor.inflate(compatibleIn, uncompressed, frame.getUncompressedSize());
      return uncompressed;
    } catch (Exception e) {
      uncompressed.release();
      throw e;
    } finally {
      compatibleIn.release();
    }
  }

  private void encodePayload(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out)
      throws DataFormatException {
    int uncompressed = msg.readableBytes();
    if (uncompressed < threshold) {
      // Under the threshold, there is nothing to do.
      ProtocolUtils.writeVarInt(out, uncompressed + 1);
      out.writeByte(0);
      out.writeBytes(msg);
    } else {
      handleCompressed(ctx, msg, out);
    }
  }

  private void handleCompressed(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out)
      throws DataFormatException {
    int uncompressed = msg.readableBytes();

    out.writeMedium(0); // Reserve the packet length
    ProtocolUtils.writeVarInt(out, uncompressed);
    ByteBuf compatibleIn = MoreByteBufUtils.ensureCompatible(ctx.alloc(), compressor, msg);

    int startCompressed = out.writerIndex();
    try {
      compressor.deflate(compatibleIn, out);
    } finally {
      compatibleIn.release();
    }
    int compressedLength = out.writerIndex() - startCompressed;
    if (compressedLength >= 1 << 21) {
      throw new DataFormatException("The server sent a very large (over 2MiB compressed) packet.");
    }

    int packetLength = out.readableBytes() - 3;
    out.setMedium(0, ProtocolUtils.encode21BitVarInt(packetLength)); // Rewrite packet length
  }

  @Override
  protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, Object message, boolean preferDirect)
      throws Exception {
    if (message instanceof CompressedFrame frame) {
      if (canForward(frame)) {
        int length = frame.content().readableBytes();
        return ctx.alloc().directBuffer(length + ProtocolUtils.varIntBytes(length));
      }
      int uncompressed = frame.getUncompressedSize();
      return MoreByteBufUtils.preferredBuffer(ctx.alloc(), compressor,
          (uncompressed - 1) + 3 + ProtocolUtils.varIntBytes(uncompressed));
    }

    ByteBuf msg = (ByteBuf) message;
    int uncompressed = msg.readableBytes();
    if (uncompressed < threshold) {
      int finalBufferSize = uncompressed + 1;
      finalBufferSize += ProtocolUtils.varIntBytes(finalBufferSize);
      return IS_JAVA_CIPHER
          ? ctx.alloc().heapBuffer(finalBufferSize)
          : ctx.alloc().directBuffer(finalBufferSize);
    }

    // (maximum data length after compression) + packet length varint + uncompressed data varint
    int initialBufferSize = (uncompressed - 1) + 3 + ProtocolUtils.varIntBytes(uncompressed);
    return MoreByteBufUtils.preferredBuffer(ctx.alloc(), compressor, initialBufferSize);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    compressor.close();
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }
}
