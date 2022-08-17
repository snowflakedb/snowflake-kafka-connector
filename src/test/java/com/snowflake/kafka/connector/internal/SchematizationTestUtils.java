package com.snowflake.kafka.connector.internal;

import java.util.HashMap;
import java.util.Map;

public class SchematizationTestUtils {
  public static final String AVRO_SCHEMA_FOR_TABLE_CREATION =
      "{\n"
          + " \"type\":\"record\",\n"
          + " \"name\":\"value_schema\",\n"
          + " \"fields\":[\n"
          + "   {\"name\":\"id\",\"type\":\"int\"},\n"
          + "   {\"name\":\"first_name\",\"type\":\"string\"},\n"
          + "   {\"name\":\"rating\",\"type\":\"float\"},\n"
          + "   {\"name\":\"approval\",\"type\":\"boolean\"},\n"
          + "   {\"name\":\"info_map\",\"type\":{\"type\":\"map\",\"values\":\"int\"}}\n"
          + " ]\n"
          + "}";

  public static final Map<String, String> SF_SCHEMA_FOR_TABLE_CREATION;

  static {
    SF_SCHEMA_FOR_TABLE_CREATION = new HashMap<>();
    SF_SCHEMA_FOR_TABLE_CREATION.put("ID", "NUMBER");
    SF_SCHEMA_FOR_TABLE_CREATION.put("FIRST_NAME", "VARCHAR");
    SF_SCHEMA_FOR_TABLE_CREATION.put("RATING", "FLOAT");
    SF_SCHEMA_FOR_TABLE_CREATION.put("APPROVAL", "BOOLEAN");
    SF_SCHEMA_FOR_TABLE_CREATION.put("INFO_MAP", "VARIANT");
    SF_SCHEMA_FOR_TABLE_CREATION.put("RECORD_METADATA", "VARIANT");
  }

  public static final Map<String, Object> CONTENT_FOR_TABLE_CREATION;

  static {
    CONTENT_FOR_TABLE_CREATION = new HashMap<>();
    CONTENT_FOR_TABLE_CREATION.put("ID", (long) 42);
    CONTENT_FOR_TABLE_CREATION.put("FIRST_NAME", "zekai");
    CONTENT_FOR_TABLE_CREATION.put("RATING", 0.99);
    CONTENT_FOR_TABLE_CREATION.put("APPROVAL", true);
    CONTENT_FOR_TABLE_CREATION.put("INFO_MAP", "{\"field\":3}");
    CONTENT_FOR_TABLE_CREATION.put("RECORD_METADATA", "RECORD_METADATA_PLACE_HOLDER");
  }
  // the difference from the content we sent is caused by the type conversion in sf

  public static final String AVRO_SCHEMA_FOR_SCHEMA_COLLECTION_0 =
      "{\n"
          + " \"type\":\"record\",\n"
          + " \"name\":\"value_schema_0\",\n"
          + " \"fields\":[\n"
          + "   {\"name\":\"id\",\"type\":\"int\"},\n"
          + "   {\"name\":\"first_name\",\"type\":\"string\"}\n"
          + " ]\n"
          + "}";

  public static final String AVRO_SCHEMA_FOR_SCHEMA_COLLECTION_1 =
      "{\n"
          + " \"type\":\"record\",\n"
          + " \"name\":\"value_schema_1\",\n"
          + " \"fields\":[\n"
          + "   {\"name\":\"id\",\"type\":\"int\"},\n"
          + "   {\"name\":\"last_name\",\"type\":\"string\"}\n"
          + " ]\n"
          + "}";
}
