/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 *
 * This file contains utility methods copied from the Snowflake Ingest SDK v1 (utils/Utils.java)
 * to support client-side validation in Kafka Connector v4.
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
