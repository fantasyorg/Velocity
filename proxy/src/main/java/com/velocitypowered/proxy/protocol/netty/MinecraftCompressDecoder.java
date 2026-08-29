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

import static com.velocitypowered.natives.util.MoreByteBufUtils.ensureCompatible;
import static com.velocitypowered.natives.util.MoreByteBufUtils.preferredBuffer;
import static com.velocitypowered.proxy.protocol.util.NettyPreconditions.checkFrame;

import com.velocitypowered.natives.compression.VelocityCompressor;
import com.velocitypowered.proxy.network.limiter.PacketLimiter;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.util.except.QuietDecoderException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.jspecify.annotations.Nullable;

/**
 * Decompresses a Minecraft packet.
 *
 * <p>With a {@link MinecraftDecoder} attached and passthrough enabled (clientbound only), a
 * compressed frame whose packet the proxy does not decode is not inflated at all: only enough of
 * the payload is inflated to read the packet id, and the frame travels on as a
 * {@link CompressedFrame} to be written to the player exactly as it arrived.
 */
public class MinecraftCompressDecoder extends MessageToMessageDecoder<ByteBuf> {

  private static final int SERVERBOUND_MAXIMUM_UNCOMPRESSED_SIZE = 2 * 1024 * 1024; // 2MiB
  private static final int VANILLA_MAXIMUM_UNCOMPRESSED_SIZE = 8 * 1024 * 1024; // 8MiB
  private static final int HARD_MAXIMUM_UNCOMPRESSED_SIZE = 128 * 1024 * 1024; // 128MiB

  private static final int CLIENTBOUND_UNCOMPRESSED_CAP =
      Boolean.getBoolean("velocity.increased-compression-cap")
          ? HARD_MAXIMUM_UNCOMPRESSED_SIZE : VANILLA_MAXIMUM_UNCOMPRESSED_SIZE;
  private static final int SERVERBOUND_UNCOMPRESSED_CAP =
          Boolean.getBoolean("velocity.increased-compression-cap")
                  ? HARD_MAXIMUM_UNCOMPRESSED_SIZE : SERVERBOUND_MAXIMUM_UNCOMPRESSED_SIZE;

  private static final boolean SKIP_COMPRESSION_VALIDATION = Boolean.getBoolean("velocity.skip-uncompressed-packet-size-validation");

  /** A packet id is a varint of at most 5 bytes; that is all the peek ever needs to inflate. */
  private static final int PEEK_BYTES = 5;

  private final ProtocolUtils.Direction direction;
  private int threshold;
  private final VelocityCompressor compressor;
  @Nullable
  private PacketLimiter packetLimiter;

  private final @Nullable MinecraftDecoder minecraftDecoder;
  private final boolean passthrough;
  private @Nullable Inflater peekInflater;
  private final byte[] peekBuffer = new byte[PEEK_BYTES];

  /**
   * Creates a new {@code MinecraftCompressDecoder} with the specified compression {@code threshold}.
   *
   * @param threshold the threshold for compression. Packets with uncompressed size below this threshold will not be compressed.
   * @param compressor the compressor instance to use
   * @param direction the direction of the packets being decoded
   */
  public MinecraftCompressDecoder(int threshold, VelocityCompressor compressor, ProtocolUtils.Direction direction) {
    this(threshold, compressor, direction, null, false);
  }

  /**
   * Creates a new {@code MinecraftCompressDecoder} that may forward undecoded frames compressed.
   *
   * @param threshold the threshold for compression
   * @param compressor the compressor instance to use
   * @param direction the direction of the packets being decoded
   * @param minecraftDecoder the decoder that knows which packets the proxy needs to read
   * @param passthrough whether undecoded clientbound frames may skip inflation
   */
  public MinecraftCompressDecoder(int threshold, VelocityCompressor compressor,
      ProtocolUtils.Direction direction, @Nullable MinecraftDecoder minecraftDecoder,
      boolean passthrough) {
    this.threshold = threshold;
    this.compressor = compressor;
    this.direction = direction;
    this.minecraftDecoder = minecraftDecoder;
    this.passthrough = passthrough && minecraftDecoder != null
        && direction == ProtocolUtils.Direction.CLIENTBOUND;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    int frameStart = in.readerIndex();
    int claimedUncompressedSize = ProtocolUtils.readVarInt(in);
    if (claimedUncompressedSize == 0) {
      if (!SKIP_COMPRESSION_VALIDATION) {
        int actualUncompressedSize = in.readableBytes();
        checkFrame(actualUncompressedSize < threshold, "Actual uncompressed size %s is greater than"
            + " threshold %s", actualUncompressedSize, threshold);
      }
      // This message is not compressed.
      if (packetLimiter != null && !packetLimiter.account(in.readableBytes())) {
        throw new QuietDecoderException("Rate limit exceeded while processing packets for %s"
            .formatted(ctx.channel().remoteAddress()));
      }
      out.add(in.retain());
      return;
    }

    checkFrame(claimedUncompressedSize >= threshold, "Uncompressed size %s is less than"
        + " threshold %s", claimedUncompressedSize, threshold);
    if (direction == ProtocolUtils.Direction.CLIENTBOUND) {
      checkFrame(claimedUncompressedSize <= CLIENTBOUND_UNCOMPRESSED_CAP,
              "Uncompressed size %s exceeds hard threshold of %s", claimedUncompressedSize,
              CLIENTBOUND_UNCOMPRESSED_CAP);
    } else {
      checkFrame(claimedUncompressedSize <= SERVERBOUND_UNCOMPRESSED_CAP,
              "Uncompressed size %s exceeds hard threshold of %s", claimedUncompressedSize,
              SERVERBOUND_UNCOMPRESSED_CAP);
    }

    if (passthrough) {
      int packetId = peekPacketId(in);
      if (packetId >= 0 && minecraftDecoder.isPassthroughCandidate(packetId)) {
        // Hand the frame on untouched, reader back at the uncompressed-size varint.
        ByteBuf frame = in.retainedSlice(frameStart, in.writerIndex() - frameStart);
        out.add(new CompressedFrame(frame, packetId, claimedUncompressedSize, threshold,
            minecraftDecoder.getProtocolVersion()));
        return;
      }
    }

    ByteBuf compatibleIn = ensureCompatible(ctx.alloc(), compressor, in);
    ByteBuf uncompressed = preferredBuffer(ctx.alloc(), compressor, claimedUncompressedSize);
    try {
      compressor.inflate(compatibleIn, uncompressed, claimedUncompressedSize);
      checkFrame(uncompressed.writerIndex() == claimedUncompressedSize,
              "Decompressed size %s does not match claimed uncompressed size %s", uncompressed.writerIndex(), claimedUncompressedSize);
      if (packetLimiter != null && !packetLimiter.account(claimedUncompressedSize)) {
        throw new QuietDecoderException("Rate limit exceeded while processing packets for %s"
            .formatted(ctx.channel().remoteAddress()));
      }
      out.add(uncompressed);
    } catch (Exception e) {
      uncompressed.release();
      throw e;
    } finally {
      compatibleIn.release();
    }
  }

  /**
   * Inflates just the head of the payload and reads the packet id varint from it. Does not move
   * the reader index. Returns -1 when the id cannot be read this way; the caller then inflates the
   * whole frame as usual, which reports any real corruption with the usual error.
   */
  private int peekPacketId(ByteBuf in) {
    Inflater inflater = this.peekInflater;
    if (inflater == null) {
      inflater = this.peekInflater = new Inflater();
    } else {
      inflater.reset();
    }

    try {
      ByteBuffer input = in.nioBuffer(in.readerIndex(), in.readableBytes());
      inflater.setInput(input);

      int produced = 0;
      while (produced < PEEK_BYTES) {
        int n = inflater.inflate(peekBuffer, produced, PEEK_BYTES - produced);
        if (n == 0 && (inflater.finished() || inflater.needsInput() || inflater.needsDictionary())) {
          break;
        }
        produced += n;
      }

      int id = 0;
      for (int i = 0; i < produced; i++) {
        int b = peekBuffer[i];
        id |= (b & 0x7F) << (7 * i);
        if ((b & 0x80) == 0) {
          return id;
        }
      }
      return -1;
    } catch (DataFormatException e) {
      return -1;
    }
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    compressor.close();
    if (peekInflater != null) {
      peekInflater.end();
      peekInflater = null;
    }
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }

  public void setPacketLimiter(@Nullable PacketLimiter packetLimiter) {
    this.packetLimiter = packetLimiter;
  }
}
