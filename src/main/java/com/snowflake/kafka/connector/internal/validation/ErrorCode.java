/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/utils/ErrorCode.java
 *
 * Modifications:
 * - Only validation-related error codes retained (INVALID_FORMAT_ROW, INVALID_VALUE_ROW, UNKNOWN_DATA_TYPE, UNSUPPORTED_DATA_TYPE, IO_ERROR, INTERNAL_ERROR)
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 *
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

/** Ingest SDK internal error codes (validation subset) */
public enum ErrorCode {
  INTERNAL_ERROR("0001"),
  INVALID_FORMAT_ROW("0004"),
  UNKNOWN_DATA_TYPE("0005"),
  IO_ERROR("0020"),
  UNSUPPORTED_DATA_TYPE("0029"),
  INVALID_VALUE_ROW("0030");

  public static final String errorMessageResource =
      "com.snowflake.kafka.connector.internal.validation.ingest_error_messages";

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
