package com.snowflake.kafka.connector.internal.telemetry;

/**
 * Placeholder for all constants used for Sending information from Connector to Snowflake through
 * Telemetry API
 */
public final class TelemetryConstants {
  public static final String TABLE_NAME = "table_name";
  public static final String CONNECTOR_NAME = "connector_name";

  public static final String PROCESSED_OFFSET = "processed-offset";

  public static final String START_TIME = "start_time";
  public static final String UNIX_TIME = "unix_time";
  public static final String ERROR_DETAIL = "error_detail";

  // ************ Streaming Constants ************//
  public static final String OFFSET_PERSISTED_IN_SNOWFLAKE = "persisted-in-snowflake-offset";
  public static final String LATEST_CONSUMER_OFFSET = "latest-consumer-offset";

  public static final String TOPIC_PARTITION_CHANNEL_NAME = "topic_partition_channel_name";
  public static final String TOPIC_PARTITION_CHANNEL_CREATION_TIME =
      "topic_partition_channel_creation_time";
  public static final String TOPIC_PARTITION_CHANNEL_CLOSE_TIME =
      "topic_partition_channel_close_time";
  public static final String VALIDATION_FAILURE_COUNT = "validation_failure_count";
  public static final String ERROR_TOLERATED_COUNT = "error_tolerated_count";
  public static final String CHANNEL_RECOVERY_COUNT = "channel_recovery_count";
  public static final String VALIDATION_DISABLED = "validation_disabled";
  public static final String ROWS_INSERTED_COUNT = "rows_inserted_count";
  public static final String ROWS_PARSED_COUNT = "rows_parsed_count";
  public static final String ROWS_ERROR_COUNT = "rows_error_count";
  public static final String SERVER_AVG_PROCESSING_LATENCY_MS = "server_avg_processing_latency_ms";
  public static final String DATABASE_NAME = "database_name";
  public static final String SCHEMA_NAME = "schema_name";
  public static final String PIPE_NAME = "pipe_name";
  public static final String STATUS_CODE = "status_code";
  public static final String LAST_ERROR_TIMESTAMP = "last_error_timestamp";
  public static final String LAST_ERROR_OFFSET_TOKEN_UPPER_BOUND =
      "last_error_offset_token_upper_bound";
  public static final String BACKPRESSURE_RETRY_COUNT = "backpressure_retry_count";
  public static final String APPEND_ROW_FALLBACK_COUNT = "append_row_fallback_count";
  public static final String CLIENT_RECREATION_COUNT = "client_recreation_count";
  public static final String SCHEMA_EVOLUTION_FAILURE_COUNT = "schema_evolution_failure_count";
  // ********** ^ Streaming Constants ^ **********//
}
