/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 *
 * This file is copied from the Snowflake Ingest SDK v1 (streaming/internal/BinaryStringUtils.java)
 * to support client-side validation in Kafka Connector v4.
 * Only the unicodeCharactersCount() method is retained as it's the only method used by validation.
 */

package com.snowflake.kafka.connector.internal.validation;

public class BinaryStringUtils {
  /** Returns the number of unicode code points in a string */
  public static int unicodeCharactersCount(String s) {
    return s.codePointCount(0, s.length());
  }
}
