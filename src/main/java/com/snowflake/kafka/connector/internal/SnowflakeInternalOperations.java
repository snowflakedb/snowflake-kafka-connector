package com.snowflake.kafka.connector.internal;

/**
 * Enum placeholders for internal operations. Useful mostly in logging and helps figure out where
 * retries are happening.
 */
public enum SnowflakeInternalOperations {
  /* Put api (uploadStream) for GCS */
  UPLOAD_FILE_TO_INTERNAL_STAGE,

  /* Put API for AWS and Azure which caches the crdentials. */
  UPLOAD_FILE_TO_INTERNAL_STAGE_NO_CONNECTION,

  /* Broken records or (Failed ingestion files) or (files in internal stage for > 1 hour ) */
  UPLOAD_FILE_TO_TABLE_STAGE,

  /* Either because of a purge file since they were ingested in SF table or because they were moved to table stage */
  REMOVE_FILE_FROM_INTERNAL_STAGE,

  /* Snowpipe REST API Usage */
  INSERT_FILES_SNOWPIPE_API,

  INSERT_REPORT_SNOWPIPE_API,

  LOAD_HISTORY_SCAN_SNOWPIPE_API,
}
