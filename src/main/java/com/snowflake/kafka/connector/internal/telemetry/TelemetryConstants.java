package com.snowflake.kafka.connector.internal.telemetry;

/**
 * Placeholder for all constants used for Sending information from Connector to Snowflake through
 * Telemetry API
 */
public final class TelemetryConstants {
  public static final String TABLE_NAME = "table_name";
  public static final String STAGE_NAME = "stage_name";
  public static final String PIPE_NAME = "pipe_name";
  public static final String PARTITION_ID = "partition_id";
  public static final String CONNECTOR_NAME = "connector_name";

  public static final String PROCESSED_OFFSET = "processed-offset";
  public static final String FLUSHED_OFFSET = "flushed-offset";
  public static final String COMMITTED_OFFSET = "committed-offset";
  public static final String PURGED_OFFSET = "purged-offset";

  public static final String RECORD_NUMBER = "record_number";
  public static final String BYTE_NUMBER = "byte_number";

  public static final String FILE_COUNT_ON_STAGE = "file-count-on-stage";
  public static final String FILE_COUNT_ON_INGESTION = "file-count-on-ingestion";
  public static final String FILE_COUNT_PURGED = "file-count-purged";
  public static final String FILE_COUNT_TABLE_STAGE_INGEST_FAIL =
      "file_count_table_stage_ingest_fail";
  public static final String FILE_COUNT_TABLE_STAGE_BROKEN_RECORD =
      "file-count-table-stage-broken-record";

  public static final String CLEANER_RESTART_COUNT = "cleaner_restart_count";

  public static final String MEMORY_USAGE = "memory_usage";

  public static final String AVERAGE_KAFKA_LAG_MS = "average_kafka_lag";
  public static final String AVERAGE_KAFKA_LAG_RECORD_COUNT = "average_kafka_lag_record_count";

  public static final String AVERAGE_INGESTION_LAG_MS = "average_ingestion_lag";
  public static final String AVERAGE_INGESTION_LAG_FILE_COUNT = "average_ingestion_lag_file_count";

  public static final String AVERAGE_COMMIT_LAG_MS = "average_commit_lag";
  public static final String AVERAGE_COMMIT_LAG_FILE_COUNT = "average_commit_lag_file_count";

  public static final String START_TIME = "start_time";
  public static final String END_TIME = "end_time";

  public static final String IS_REUSE_TABLE = "is_reuse_table";
  public static final String IS_REUSE_STAGE = "is_reuse_stage";
  public static final String IS_REUSE_PIPE = "is_reuse_pipe";
  public static final String FILE_COUNT_RESTART = "file_count_restart";
  public static final String FILE_COUNT_REPROCESS_PURGE = "file_count_reprocess_purge";

  // ************ Streaming Constants ************//
  public static final String OFFSET_PERSISTED_IN_SNOWFLAKE = "persisted-in-snowflake-offset";
  public static final String LATEST_CONSUMER_OFFSET = "latest-consumer-offset";

  public static final String TOPIC_PARTITION_CHANNEL_NAME = "topic_partition_channel_name";
  public static final String TOPIC_PARTITION_CHANNEL_CREATION_TIME =
      "topic_partition_channel_creation_time";
  public static final String TOPIC_PARTITION_CHANNEL_CLOSE_TIME =
      "topic_partition_channel_close_time";
  // ********** ^ Streaming Constants ^ **********//
}
