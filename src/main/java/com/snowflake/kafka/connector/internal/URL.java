package com.snowflake.kafka.connector.internal;

public interface URL {
  String hostWithPort();

  String getScheme();

  default boolean sslEnabled() {
    return "https".equals(getScheme());
  }

  default String path() {
    return "";
  }
}
