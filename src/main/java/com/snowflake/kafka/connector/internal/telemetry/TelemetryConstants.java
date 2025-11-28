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

  public static final String IS_REUSE_TABLE = "is_reuse_table";

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
