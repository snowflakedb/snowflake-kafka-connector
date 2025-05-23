package com.snowflake.kafka.connector.internal.streaming.validation;

public class RowsetApiException extends RuntimeException {
  public RowsetApiException(String message) {
    super(message);
  }

  public RowsetApiException(Throwable cause) {
    super(cause);
  }
}
