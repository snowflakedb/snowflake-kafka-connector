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

  /** List */
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
}
