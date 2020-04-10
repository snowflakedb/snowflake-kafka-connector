package com.snowflake.reporter;

import com.snowflake.Utils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;

/*
{
    "connector" : "kafka-connector",
    "time" : 12345678(timestamp),
    "test_suites" : [
        {
            "name" : "json"
            "test_cases" : [
                {
                    "name": "one G table",
                    "table_name": "one_g_table"
                    "time": 12321.222
                }
            ]
        },
        {
            "name" : "avro_with_schema_registry",
            "test_cases" : []
        }
    ]
}
*/

public abstract class Reporter
{
  static final String NAME = "name";
  static final String TIME = "time";
  static final String TABLE_NAME = "table_name";
  static final String TEST_CASES = "test_cases";
  static final String TEST_SUITES = "test_suites";
  static final String CONNECTOR = "connector";

  private static final String APP_NAME = "kafka-connector";

  static final ObjectMapper MAPPER = new ObjectMapper();

  abstract public JsonNode getNode();

  String getAppName()
  {
    if (Utils.TEST_MODE)
    {
      return APP_NAME.concat("_test");
    }
    else
    {
      return APP_NAME;
    }
  }

  @Override
  public String toString()
  {
    return getNode().toString();
  }


  public JsonArray toJsonArray()
  {
    return new JsonArray(this);
  }

  public JsonArray add(Reporter other)
  {
    return toJsonArray().add(other);
  }

}