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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.natives.compression.JavaVelocityCompressor;
import com.velocitypowered.natives.compression.VelocityCompressor;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.zip.Deflater;
import org.junit.jupiter.api.Test;

/**
 * A backend frame the proxy does not decode must reach the player exactly as it arrived when both
 * sides share the compression threshold, and must take the inflate/deflate path otherwise.
 */
class CompressedPassthroughTest {

  private static final int THRESHOLD = 256;
  private static final ProtocolVersion VERSION = ProtocolVersion.MAXIMUM_VERSION;

  private static MinecraftDecoder playDecoder() {
    MinecraftDecoder decoder = new MinecraftDecoder(ProtocolUtils.Direction.CLIENTBOUND);
    decoder.setProtocolVersion(VERSION);
    decoder.setState(StateRegistry.PLAY);
    return decoder;
  }

  /** An id the proxy has no packet class for in PLAY: the situation of chunks and entities. */
  private static int unregisteredId(MinecraftDecoder decoder) {
    for (int id = 0x20; id < 0x7F; id++) {
      if (decoder.isPassthroughCandidate(id)) {
        return id;
      }
    }
    throw new AssertionError("every id is registered?");
  }

  /** An id the proxy does decode (keep alive), which must never travel compressed. */
  private static int registeredId(MinecraftDecoder decoder) {
    for (int id = 0; id < 0x7F; id++) {
      if (!decoder.isPassthroughCandidate(id)) {
        return id;
      }
    }
    throw new AssertionError("no registered id?");
  }

  private static byte[] payload(int packetId, int size) {
    ByteBuf buf = Unpooled.buffer();
    ProtocolUtils.writeVarInt(buf, packetId);
    for (int i = 0; buf.readableBytes() < size; i++) {
      buf.writeByte(i % 7); // repetitive so it actually compresses
    }
    return ByteBufUtil.getBytes(buf);
  }

  /** The frame as the backend sends it, without the outer length: size varint + deflate data. */
  private static ByteBuf compressedFrame(byte[] payload) {
    Deflater deflater = new Deflater();
    deflater.setInput(payload);
    deflater.finish();
    byte[] out = new byte[payload.length + 64];
    int compressed = deflater.deflate(out);
    deflater.end();

    ByteBuf frame = Unpooled.buffer();
    ProtocolUtils.writeVarInt(frame, payload.length);
    frame.writeBytes(out, 0, compressed);
    return frame;
  }

  @Test
  void undecodedFrameStaysCompressed() {
    MinecraftDecoder decoder = playDecoder();
    int id = unregisteredId(decoder);
    byte[] payload = payload(id, 1024);
    ByteBuf frame = compressedFrame(payload);

    VelocityCompressor compressor = JavaVelocityCompressor.FACTORY.create(-1);
    EmbeddedChannel channel = new EmbeddedChannel(
        new MinecraftCompressDecoder(THRESHOLD, compressor, ProtocolUtils.Direction.CLIENTBOUND,
            decoder, true));

    final byte[] wire = ByteBufUtil.getBytes(frame);
    assertTrue(channel.writeInbound(frame));
    CompressedFrame out = assertInstanceOf(CompressedFrame.class, channel.readInbound());
    assertEquals(id, out.getPacketId());
    assertEquals(payload.length, out.getUncompressedSize());
    assertEquals(THRESHOLD, out.getThreshold());
    assertEquals(VERSION, out.getProtocolVersion());
    assertEquals(0, ByteBufUtil.compare(Unpooled.wrappedBuffer(wire), out.content()),
        "the frame must be untouched");
    out.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void decodedPacketIsInflated() {
    MinecraftDecoder decoder = playDecoder();
    int id = registeredId(decoder);
    byte[] payload = payload(id, 1024);

    VelocityCompressor compressor = JavaVelocityCompressor.FACTORY.create(-1);
    EmbeddedChannel channel = new EmbeddedChannel(
        new MinecraftCompressDecoder(THRESHOLD, compressor, ProtocolUtils.Direction.CLIENTBOUND,
            decoder, true));

    assertTrue(channel.writeInbound(compressedFrame(payload)));
    ByteBuf out = assertInstanceOf(ByteBuf.class, channel.readInbound());
    assertEquals(0, ByteBufUtil.compare(Unpooled.wrappedBuffer(payload), out));
    out.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void passthroughDisabledInflatesEverything() {
    MinecraftDecoder decoder = playDecoder();
    byte[] payload = payload(unregisteredId(decoder), 1024);

    VelocityCompressor compressor = JavaVelocityCompressor.FACTORY.create(-1);
    EmbeddedChannel channel = new EmbeddedChannel(
        new MinecraftCompressDecoder(THRESHOLD, compressor, ProtocolUtils.Direction.CLIENTBOUND,
            decoder, false));

    assertTrue(channel.writeInbound(compressedFrame(payload)));
    ByteBuf out = assertInstanceOf(ByteBuf.class, channel.readInbound());
    out.release();
    channel.finishAndReleaseAll();
  }

  @Test
  void sameThresholdWritesFrameVerbatim() {
    byte[] payload = payload(0x42, 1024);
    ByteBuf frame = compressedFrame(payload);

    EmbeddedChannel channel = new EmbeddedChannel(new MinecraftCompressorAndLengthEncoder(THRESHOLD,
        JavaVelocityCompressor.FACTORY.create(-1)));
    final byte[] wire = ByteBufUtil.getBytes(frame);
    final long passedBefore = CompressedFrameStats.passedFrames();
    assertTrue(channel.writeOutbound(new CompressedFrame(frame, 0x42, payload.length, THRESHOLD,
        VERSION)));

    ByteBuf out = channel.readOutbound();
    assertEquals(wire.length, ProtocolUtils.readVarInt(out));
    assertEquals(0, ByteBufUtil.compare(Unpooled.wrappedBuffer(wire), out));
    assertEquals(passedBefore + 1, CompressedFrameStats.passedFrames());
    out.release();
    channel.finishAndReleaseAll();
  }

  /**
   * Deflate is deterministic, so a rebuilt frame can be byte-identical to the original; what tells
   * the two paths apart is the counter each one bumps.
   */
  @Test
  void differentThresholdRecompresses() {
    byte[] payload = payload(0x42, 1024);
    ByteBuf frame = compressedFrame(payload);
    int playerThreshold = 512;

    EmbeddedChannel channel = new EmbeddedChannel(new MinecraftCompressorAndLengthEncoder(
        playerThreshold, JavaVelocityCompressor.FACTORY.create(-1)));
    final long passedBefore = CompressedFrameStats.passedFrames();
    final long recompressedBefore = CompressedFrameStats.recompressedFrames();
    assertTrue(channel.writeOutbound(new CompressedFrame(frame, 0x42, payload.length, THRESHOLD,
        VERSION)));

    ByteBuf out = channel.readOutbound();
    int length = ProtocolUtils.readVarInt(out);
    assertEquals(length, out.readableBytes());
    assertEquals(passedBefore, CompressedFrameStats.passedFrames());
    assertEquals(recompressedBefore + 1, CompressedFrameStats.recompressedFrames());

    // What the player receives must decode back to the original payload under its own threshold.
    EmbeddedChannel playerSide = new EmbeddedChannel(new MinecraftCompressDecoder(playerThreshold,
        JavaVelocityCompressor.FACTORY.create(-1), ProtocolUtils.Direction.CLIENTBOUND));
    assertTrue(playerSide.writeInbound(out));
    ByteBuf decoded = assertInstanceOf(ByteBuf.class, playerSide.readInbound());
    assertEquals(0, ByteBufUtil.compare(Unpooled.wrappedBuffer(payload), decoded));
    decoded.release();
    playerSide.finishAndReleaseAll();
    channel.finishAndReleaseAll();
  }

  @Test
  void forcedRecompressIsHonoured() {
    byte[] payload = payload(0x42, 1024);
    ByteBuf frame = compressedFrame(payload);

    EmbeddedChannel channel = new EmbeddedChannel(new MinecraftCompressorAndLengthEncoder(THRESHOLD,
        JavaVelocityCompressor.FACTORY.create(-1)));
    CompressedFrame forced = new CompressedFrame(frame, 0x42, payload.length, THRESHOLD, VERSION);
    forced.setForceRecompress(true);
    final long passedBefore = CompressedFrameStats.passedFrames();
    final long recompressedBefore = CompressedFrameStats.recompressedFrames();
    assertTrue(channel.writeOutbound(forced));

    ByteBuf out = channel.readOutbound();
    assertEquals(ProtocolUtils.readVarInt(out), out.readableBytes());
    assertEquals(passedBefore, CompressedFrameStats.passedFrames());
    assertEquals(recompressedBefore + 1, CompressedFrameStats.recompressedFrames());
    assertFalse(forced.isForceRecompress() && out.readableBytes() == 0);
    out.release();
    channel.finishAndReleaseAll();
  }
}
