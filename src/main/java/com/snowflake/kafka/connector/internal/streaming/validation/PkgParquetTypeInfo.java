/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.openflow.runtime.processors.snowpipe.validation;

import org.apache.parquet.schema.Type;

import java.util.Map;

/**
 * Util class that contains Parquet type and other metadata for that type needed by the Snowflake
 * server side scanner
 */
class PkgParquetTypeInfo(Type parquetType, Map<String, String> metadata) {

}
