package com.snowflake.kafka.connector.internal;

public class SnowflakeKafkaConnectorException extends RuntimeException {
  private final String code;

  public SnowflakeKafkaConnectorException(String msg, String code) {
    super(msg);
    this.code = code;
  }

  public String getCode() {
    return code;
  }

  public boolean checkErrorCode(SnowflakeErrors error) {
    return this.code.equals(error.getCode());
  }
}
