/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

/** Ingest SDK internal error codes */
public enum ErrorCode {
  INTERNAL_ERROR("0001"),
  INVALID_FORMAT_ROW("0004"),
  UNKNOWN_DATA_TYPE("0005"),
  IO_ERROR("0020"),
  UNSUPPORTED_DATA_TYPE("0029"),
  INVALID_VALUE_ROW("0030");

  public static final String errorMessageResource =
      "com.snowflake.kafka.connector.ingest_error_messages";

  /** Snowflake internal message associated to the error. */
  private final String messageCode;

  /**
   * Construct a new error code specification given Snowflake internal error code.
   *
   * @param messageCode Snowflake internal error code
   */
  ErrorCode(String messageCode) {
    this.messageCode = messageCode;
  }

  public String getMessageCode() {
    return messageCode;
  }

  @Override
  public String toString() {
    return "ErrorCode{" + "name=" + this.name() + ", messageCode=" + messageCode + "}";
  }
}
