package com.snowflake.kafka.connector.internal;

public class SnowflakeKafkaConnectorException extends RuntimeException
{
  public SnowflakeKafkaConnectorException(String msg)
  {
    super(msg);
  }
}
