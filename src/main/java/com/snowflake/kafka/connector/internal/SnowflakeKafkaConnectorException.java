package com.snowflake.kafka.connector.internal;

public class SnowflakeKafkaConnectorException extends RuntimeException {
  private final String code;
  private final String userMessage;

  public SnowflakeKafkaConnectorException(String msg, String code) {
    super(msg);
    this.code = code;
    this.userMessage = msg;
  }

  public SnowflakeKafkaConnectorException(String msg, String code, String userMessage) {
    super(msg);
    this.code = code;
    this.userMessage = userMessage;
  }

  public String getCode() {
    return code;
  }

  public boolean checkErrorCode(SnowflakeErrors error) {
    return this.code.equals(error.getCode());
  }

  public String getExceptionUserMessage() {
    return this.userMessage;
  }
}
