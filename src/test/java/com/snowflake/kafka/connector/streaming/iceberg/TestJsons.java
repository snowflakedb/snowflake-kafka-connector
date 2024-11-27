package com.snowflake.kafka.connector.streaming.iceberg;

/**
 * Class to provide Iceberg schema evolution tests with schemas and payload. It decreases the size
 * of the test file .
 */
class TestJsons {

  public static String schemaNestedObjects(String payload) {
    return "{"
        + " \"schema\": {"
        + "  \"type\": \"struct\","
        + "  \"fields\": ["
        + "   {"
        + "    \"field\": \"object_With_Nested_Objects\","
        + "    \"type\": \"struct\","
        + "    \"fields\": ["
        + "     {"
        + "      \"field\": \"nestedStruct\","
        + "      \"type\": \"struct\","
        + "      \"fields\": ["
        + "       {"
        + "        \"field\": \"description\","
        + "        \"type\": \"string\""
        + "       }"
        + "      ]"
        + "     }"
        + "    ]"
        + "   }"
        + "  ],"
        + "  \"optional\": true,"
        + "  \"name\": \"sf.kc.test\""
        + " },"
        + " \"payload\": "
        + payload
        + "}";
  }

  static String nestedObjectsPayload =
      "{"
          + "    \"object_With_Nested_Objects\": {"
          + "      \"nestedStruct\": {"
          + "        \"description\": \"txt\""
          + "      }"
          + "    }"
          + "  }";

  static String simpleMapSchema(String payload) {
    return "{"
        + "    \"schema\": {"
        + "        \"type\": \"struct\","
        + "        \"fields\": ["
        + "            {"
        + "                \"field\": \"simple_Test_Map\","
        + "                \"type\": \"map\","
        + "                \"keys\": {"
        + "                    \"type\": \"string\""
        + "                },"
        + "                \"values\": {"
        + "                    \"type\": \"int32\""
        + "                }"
        + "            }"
        + "        ]"
        + "    },"
        + "       \"payload\":"
        + payload
        + "}";
  }

  static String simpleMapPayload =
      "{"
          + "        \"simple_Test_Map\": {"
          + "            \"key1\": 12,"
          + "            \"key2\": 15"
          + "        }"
          + "    }";

  static String simpleArraySchema(String payload) {
    return "{\n"
        + "    \"schema\": {\n"
        + "        \"type\": \"struct\",\n"
        + "        \"fields\": [\n"
        + "            {\n"
        + "                \"field\": \"simple_Array\",\n"
        + "                \"type\": \"array\",\n"
        + "                \"items\": {\n"
        + "                    \"type\": \"int32\"\n"
        + "                }\n"
        + "            }\n"
        + "        ]\n"
        + "    },\n"
        + "    \"payload\":"
        + payload
        + "}";
  }

  static String simpleArrayPayload = "{ \"simple_Array\": [ 1,2,3] } ";

  /** Object containing a list of maps */
  static String complexSchema(String payload) {
    return "{"
        + "    \"schema\": {"
        + "        \"type\": \"struct\","
        + "        \"fields\": ["
        + "            {"
        + "                \"field\": \"object\","
        + "                \"type\": \"struct\","
        + "                \"fields\": ["
        + "                    {"
        + "                        \"field\": \"arrayOfMaps\","
        + "                        \"type\": \"array\","
        + "                        \"items\": {"
        + "                            \"field\": \"simple_Test_Map\","
        + "                            \"type\": \"map\","
        + "                            \"keys\": {"
        + "                                \"type\": \"string\""
        + "                            },"
        + "                            \"values\": {"
        + "                                \"type\": \"float\""
        + "                            }"
        + "                        }"
        + "                    }"
        + "                ]"
        + "            }"
        + "        ]"
        + "    },"
        + "    \"payload\":"
        + payload
        + "}";
  }

  static String complexPayload =
      "{"
          + "        \"object\": {"
          + "            \"arrayOfMaps\": ["
          + "                {"
          + "                    \"simple_Test_Map\": {"
          + "                        \"keyString\": 3.14 "
          + "                    }"
          + "                }"
          + "            ]"
          + "        }"
          + "    }";

  static String singleBooleanField() {
    return SCHEMA_BEGINNING + BOOL_SCHEMA + SCHEMA_END + "\"payload\": {" + BOOL_PAYLOAD + "}}";
  }

  static String booleanAndInt() {
    return SCHEMA_BEGINNING
        + BOOL_SCHEMA
        + ","
        + INT64_SCHEMA
        + SCHEMA_END
        + "\"payload\":{"
        + BOOL_PAYLOAD
        + ","
        + INT64_PAYLOAD
        + "}}";
  }

  static String booleanAndAllKindsOfInt() {
    return SCHEMA_BEGINNING
        + BOOL_SCHEMA
        + ","
        + INT64_SCHEMA
        + ","
        + INT32_SCHEMA
        + ","
        + INT16_SCHEMA
        + ","
        + INT8_SCHEMA
        + SCHEMA_END
        + "\"payload\":{"
        + BOOL_PAYLOAD
        + ","
        + INT64_PAYLOAD
        + ","
        + INT32_PAYLOAD
        + ","
        + INT16_PAYLOAD
        + ","
        + INT8_PAYLOAD
        + "}}";
  }

  static String allPrimitives() {
    return SCHEMA_BEGINNING
        + BOOL_SCHEMA
        + ","
        + INT64_SCHEMA
        + ","
        + INT32_SCHEMA
        + ","
        + INT16_SCHEMA
        + ","
        + INT8_SCHEMA
        + ","
        + FLOAT_SCHEMA
        + ","
        + DOUBLE_SCHEMA
        + ","
        + STRING_SCHEMA
        + SCHEMA_END
        + "\"payload\":{"
        + BOOL_PAYLOAD
        + ","
        + INT64_PAYLOAD
        + ","
        + INT32_PAYLOAD
        + ","
        + INT16_PAYLOAD
        + ","
        + INT8_PAYLOAD
        + ","
        + FLOAT_PAYLOAD
        + ","
        + DOUBLE_PAYLOAD
        + ","
        + STRING_PAYLOAD
        + "}}";
  }

  static String BOOL_SCHEMA = " {  \"field\" : \"test_boolean\", \"type\" : \"boolean\"} ";

  static String INT64_SCHEMA = "{  \"field\" : \"test_int64\", \"type\" : \"int64\" }";
  static String INT32_SCHEMA = "{  \"field\" : \"test_int32\", \"type\" : \"int32\" }";
  static String INT16_SCHEMA = "{  \"field\" : \"test_int16\", \"type\" : \"int16\" }";
  static String INT8_SCHEMA = "{  \"field\" : \"test_int8\", \"type\" : \"int8\" }";

  static String FLOAT_SCHEMA = "{  \"field\" : \"test_float\", \"type\" : \"float\" }";

  static String DOUBLE_SCHEMA = "{  \"field\" : \"test_double\", \"type\" : \"double\"  }";

  static String STRING_SCHEMA = "{  \"field\" : \"test_string\",  \"type\" : \"string\"  }";

  static final String BOOL_PAYLOAD = "\"test_boolean\" : true ";
  static final String INT64_PAYLOAD = "\"test_int64\" : 2137324241343241 ";
  static final String INT32_PAYLOAD = "\"test_int32\" : 2137 ";
  static final String INT16_PAYLOAD = "\"test_int16\" : 2137 ";
  static final String INT8_PAYLOAD = "\"test_int8\" : 2137 ";
  static final String FLOAT_PAYLOAD = "\"test_float\" : 1939.30 ";
  static final String DOUBLE_PAYLOAD = "\"test_double\" : 123.45793247859723 ";
  static final String STRING_PAYLOAD = "\"test_string\" : \"very long string\" ";

  private static final String SCHEMA_BEGINNING =
      "{ \"schema\": { \"type\": \"struct\", \"fields\": [";
  private static final String SCHEMA_END = "]},";
}
