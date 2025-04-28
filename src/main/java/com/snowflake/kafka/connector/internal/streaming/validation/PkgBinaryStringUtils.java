package com.snowflake.kafka.connector.internal.streaming.validation;

import java.nio.ByteBuffer;
import org.apache.commons.codec.binary.Hex;

class PkgBinaryStringUtils {
  private static final int MAX_LOB_LEN = 32;

  /** Returns the number of unicode code points in a string */
  static int unicodeCharactersCount(String s) {
    return s.codePointCount(0, s.length());
  }

  /**
   * Truncate an array of bytes to 32 bytes and optionally increment the last byte(s). More the one
   * byte can be incremented in case it overflows.
   */
  static String truncateBytesAsHex(byte[] bytes, boolean truncateUp) {
    if (bytes.length <= MAX_LOB_LEN) {
      return Hex.encodeHexString(bytes);
    }

    // In order not to mutate the original byte array, let's make a copy
    byte[] result = new byte[MAX_LOB_LEN];
    System.arraycopy(bytes, 0, result, 0, MAX_LOB_LEN);

    // Round the least significant byte(s) up
    if (truncateUp) {
      int idx;
      for (idx = MAX_LOB_LEN - 1; idx >= 0; idx--) {
        // increment the current byte, if there was no overflow, we can stop
        if (++result[idx] != 0) {
          break;
        }
      }
      // Whole prefix has overflown, return infinity
      if (idx == -1) {
        return "Z";
      }
    }

    return Hex.encodeHexString(ByteBuffer.wrap(result, 0, MAX_LOB_LEN));
  }
}
