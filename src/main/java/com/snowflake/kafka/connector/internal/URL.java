package com.snowflake.kafka.connector.internal;

public interface URL {
  String hostWithPort();

  String getScheme();

  String path();

  boolean sslEnabled();
}
