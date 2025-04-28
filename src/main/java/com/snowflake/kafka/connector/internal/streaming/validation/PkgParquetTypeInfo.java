/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import java.util.Map;
import org.apache.parquet.schema.Type;

/**
 * Util class that contains Parquet type and other metadata for that type needed by the Snowflake
 * server side scanner
 */
class PkgParquetTypeInfo {

  private final Type parquetType;

  private final Map<String, String> metadata;

  public PkgParquetTypeInfo(final Type parquetType, final Map<String, String> metadata) {
    this.parquetType = parquetType;
    this.metadata = metadata;
  }

  public Type parquetType() {
    return parquetType;
  }

  public Map<String, String> metadata() {
    return metadata;
  }
}
