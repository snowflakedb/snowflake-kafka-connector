package com.snowflake.kafka.connector.streaming.iceberg;

public class IcebergDDLTypes {

  public static String ICEBERG_METADATA_OBJECT_SCHEMA =
      "OBJECT("
          + "offset LONG,"
          + "topic STRING,"
          + "partition INTEGER,"
          + "key STRING,"
          + "CreateTime BIGINT,"
          + "SnowflakeConnectorPushTime BIGINT,"
          + "headers MAP(VARCHAR, VARCHAR)"
          + ")";
}
