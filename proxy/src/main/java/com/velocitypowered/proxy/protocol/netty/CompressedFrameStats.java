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

import java.util.concurrent.atomic.LongAdder;

/**
 * Proxy-wide counters for the compressed passthrough: how many backend frames were forwarded
 * verbatim versus inflated and compressed again. Read by the {@code /velocity info} report.
 */
public final class CompressedFrameStats {

  private static final LongAdder PASSED_FRAMES = new LongAdder();
  private static final LongAdder PASSED_BYTES = new LongAdder();
  private static final LongAdder RECOMPRESSED_FRAMES = new LongAdder();
  private static final LongAdder RECOMPRESSED_BYTES = new LongAdder();

  private CompressedFrameStats() {
    throw new AssertionError();
  }

  public static void passed(int compressedBytes) {
    PASSED_FRAMES.increment();
    PASSED_BYTES.add(compressedBytes);
  }

  public static void recompressed(int uncompressedBytes) {
    RECOMPRESSED_FRAMES.increment();
    RECOMPRESSED_BYTES.add(uncompressedBytes);
  }

  public static long passedFrames() {
    return PASSED_FRAMES.sum();
  }

  public static long passedBytes() {
    return PASSED_BYTES.sum();
  }

  public static long recompressedFrames() {
    return RECOMPRESSED_FRAMES.sum();
  }

  public static long recompressedBytes() {
    return RECOMPRESSED_BYTES.sum();
  }
}
