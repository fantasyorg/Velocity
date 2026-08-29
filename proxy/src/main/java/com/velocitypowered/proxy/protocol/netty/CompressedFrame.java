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

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.util.DeferredByteBufHolder;
import io.netty.buffer.ByteBuf;

/**
 * A compressed frame received from a backend server that the proxy does not need to decode, kept
 * exactly as it arrived on the wire (uncompressed-size varint + deflate payload). When the player
 * connection uses the same compression threshold, the frame is forwarded verbatim instead of being
 * inflated and deflated again — for chunk data, that is nearly all of the proxy's CPU.
 */
public final class CompressedFrame extends DeferredByteBufHolder {

  private final int packetId;
  private final int uncompressedSize;
  private final int threshold;
  private final ProtocolVersion protocolVersion;
  private boolean forceRecompress;

  /**
   * Creates a compressed frame.
   *
   * @param backing the frame as received (readerIndex at the uncompressed-size varint)
   * @param packetId the packet id peeked from the start of the inflated payload
   * @param uncompressedSize the uncompressed size claimed by the frame header
   * @param threshold the compression threshold of the connection that produced the frame
   * @param protocolVersion the protocol version the frame was encoded for
   */
  public CompressedFrame(ByteBuf backing, int packetId, int uncompressedSize, int threshold,
      ProtocolVersion protocolVersion) {
    super(backing);
    this.packetId = packetId;
    this.uncompressedSize = uncompressedSize;
    this.threshold = threshold;
    this.protocolVersion = protocolVersion;
  }

  public int getPacketId() {
    return packetId;
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }

  public int getThreshold() {
    return threshold;
  }

  public ProtocolVersion getProtocolVersion() {
    return protocolVersion;
  }

  /** Whether the frame must be inflated and compressed again instead of forwarded as-is. */
  public boolean isForceRecompress() {
    return forceRecompress;
  }

  public void setForceRecompress(boolean forceRecompress) {
    this.forceRecompress = forceRecompress;
  }

  @Override
  public String toString() {
    return "CompressedFrame{packetId=0x" + Integer.toHexString(packetId)
        + ", uncompressedSize=" + uncompressedSize + ", threshold=" + threshold + '}';
  }
}
