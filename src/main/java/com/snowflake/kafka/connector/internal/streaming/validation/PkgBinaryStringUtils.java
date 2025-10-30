package com.snowflake.kafka.connector.internal.streaming.validation;

class PkgBinaryStringUtils {

  /** Returns the number of unicode code points in a string */
  static int unicodeCharactersCount(String s) {
    return s.codePointCount(0, s.length());
  }
}
