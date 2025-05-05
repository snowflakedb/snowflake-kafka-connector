/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

/** Ingest SDK internal error codes */
public enum ErrorCode {
  INTERNAL_ERROR("0001"),
  NULL_VALUE("0002"),
  NULL_OR_EMPTY_STRING("0003"),
  INVALID_FORMAT_ROW("0004"),
  UNKNOWN_DATA_TYPE("0005"),
  REGISTER_BLOB_FAILURE("0006"),
  OPEN_CHANNEL_FAILURE("0007"),
  BUILD_REQUEST_FAILURE("0008"),
  CLIENT_CONFIGURE_FAILURE("0009"),
  MISSING_CONFIG("0010"),
  BLOB_UPLOAD_FAILURE("0011"),
  RESOURCE_CLEANUP_FAILURE("0012"),
  INVALID_CHANNEL("0013"),
  CLOSED_CHANNEL("0014"),
  INVALID_URL("0015"),
  CLOSED_CLIENT("0016"),
  INVALID_PRIVATE_KEY("0017"),
  INVALID_ENCRYPTED_KEY("0018"),
  INVALID_DATA_IN_CHUNK("0019"),
  IO_ERROR("0020"),
  UNABLE_TO_CONNECT_TO_STAGE("0021"),
  KEYPAIR_CREATION_FAILURE("0022"),
  MD5_HASHING_NOT_AVAILABLE("0023"),
  CHANNEL_STATUS_FAILURE("0024"),
  CHANNELS_WITH_UNCOMMITTED_ROWS("0025"),
  INVALID_COLLATION_STRING("0026"),
  ENCRYPTION_FAILURE("0027"),
  CHANNEL_STATUS_INVALID("0028"),
  UNSUPPORTED_DATA_TYPE("0029"),
  INVALID_VALUE_ROW("0030"),
  MAX_ROW_SIZE_EXCEEDED("0031"),
  MAKE_URI_FAILURE("0032"),
  OAUTH_REFRESH_TOKEN_ERROR("0033"),
  INVALID_CONFIG_PARAMETER("0034"),
  CRYPTO_PROVIDER_ERROR("0035"),
  DROP_CHANNEL_FAILURE("0036"),
  CLIENT_DEPLOYMENT_ID_MISMATCH("0037"),
  GENERATE_PRESIGNED_URLS_FAILURE("0038"),
  REFRESH_TABLE_INFORMATION_FAILURE("0039");

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
