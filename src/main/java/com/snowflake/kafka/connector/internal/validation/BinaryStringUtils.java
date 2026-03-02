/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/streaming/internal/BinaryStringUtils.java
 *
 * Modifications:
 * - Only unicodeCharactersCount() method retained (only method used by validation)
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 *
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

public class BinaryStringUtils {
  /** Returns the number of unicode code points in a string */
  public static int unicodeCharactersCount(String s) {
    return s.codePointCount(0, s.length());
  }
}
