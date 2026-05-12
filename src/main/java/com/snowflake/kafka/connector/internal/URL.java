package com.snowflake.kafka.connector.internal;

public interface URL {
  String hostWithPort();

  String getScheme();

  boolean sslEnabled();

  String path();
}
