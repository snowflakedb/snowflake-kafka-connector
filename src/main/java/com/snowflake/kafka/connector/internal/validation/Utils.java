/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/utils/Utils.java
 *
 * Modifications:
 * - Only stripTrailingNulls() method retained (only method used by validation)
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 *
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

/** Utility methods for validation */
class Utils {
  /**
   * Strip trailing null characters from a string
   *
   * @param key input string
   * @return string with trailing nulls removed
   */
  public static String stripTrailingNulls(String key) {
    int end = key.length();
    while (end > 0 && key.charAt(end - 1) == '\u0000') {
      end--;
    }
    return end == key.length() ? key : key.substring(0, end);
  }
}
