package com.snowflake.kafka.connector.streaming.iceberg;

public class IcebergDDLTypes {
  public static String INTEGER = "INTEGER";
  public static String STRING = "STRING";
  public static String BIGINT = "BIGINT";
  public static String MAP = "MAP";
  public static String OBJECT = "OBJECT";
  public static String VARCHAR = "VARCHAR";

  public static String ICEBERG_METADATA_OBJECT_SCHEMA =
      OBJECT
          + "("
          + "offset "
          + INTEGER
          + ","
          + "topic "
          + STRING
          + ","
          + "partition "
          + INTEGER
          + ","
          + "key "
          + STRING
          + ","
          + "schema_id "
          + INTEGER
          + ","
          + "key_schema_id "
          + INTEGER
          + ","
          + "CreateTime "
          + BIGINT
          + ","
          + "LogAppendTime "
          + BIGINT
          + ","
          + "SnowflakeConnectorPushTime "
          + BIGINT
          + ","
          + "headers "
          + MAP
          + "("
          + VARCHAR
          + ","
          + VARCHAR
          + ")"
          + ")";
}
