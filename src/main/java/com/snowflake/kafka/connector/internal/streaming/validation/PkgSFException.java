/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

/** Snowflake exception in the Ingest SDK */
class PkgSFException extends RuntimeException {

  static final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  private final Throwable cause;
  private final String vendorCode;
  private final Object[] params;

  /**
   * Construct a Snowflake exception from a cause, an error code and message parameters
   *
   * @param cause
   * @param errorCode
   * @param params
   */
  public PkgSFException(Throwable cause, ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params),
        cause);

    this.vendorCode = errorCode.getMessageCode();
    this.params = params;
    this.cause = cause;
  }

  /**
   * Construct a Snowflake exception from an error code and message parameters
   *
   * @param errorCode
   * @param params
   */
  public PkgSFException(ErrorCode errorCode, Object... params) {
    this(null, errorCode, params);
  }

  public String getVendorCode() {
    return vendorCode;
  }

  public Object[] getParams() {
    return params;
  }

  public Throwable getCause() {
    return cause;
  }
}
