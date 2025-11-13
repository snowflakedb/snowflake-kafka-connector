package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.Utils;
import java.util.Map;

public class IcebergDDLTypes {

  public static String ICEBERG_METADATA_VARIANT_TYPE = "VARIANT";
  public static String ICEBERG_CONTENT_VARIANT_TYPE = "VARIANT";

  public static String ICEBERG_METADATA_OBJECT_SCHEMA =
      "OBJECT("
          + "offset LONG,"
          + "topic STRING,"
          + "partition INTEGER,"
          + "key STRING,"
          + "schema_id INTEGER,"
          + "key_schema_id INTEGER,"
          + "CreateTime BIGINT,"
          + "LogAppendTime BIGINT,"
          + "SnowflakeConnectorPushTime BIGINT,"
          + "headers MAP(VARCHAR, VARCHAR)"
          + ")";

  public static String getMetadataType(Map<String, String> config) {
    return Utils.isIcebergUseVariantType(config)
        ? ICEBERG_METADATA_VARIANT_TYPE
        : ICEBERG_METADATA_OBJECT_SCHEMA;
  }

  public static String getContentType(Map<String, String> config, String structuredSchema) {
    return Utils.isIcebergUseVariantType(config)
        ? ICEBERG_CONTENT_VARIANT_TYPE
        : structuredSchema;
  }
}
