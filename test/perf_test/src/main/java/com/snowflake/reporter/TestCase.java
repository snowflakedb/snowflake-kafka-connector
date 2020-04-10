package com.snowflake.reporter;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

public class TestCase extends Reporter
{

  private final ObjectNode node;

  public TestCase(String name, String tableName, long time)
  {
    node = MAPPER.createObjectNode();
    node.put(NAME, name);
    node.put(TABLE_NAME, tableName);
    node.put(TIME, time / 1000.0);

    System.out.println("create test case report:\n" + toString());
  }

  @Override
  public JsonNode getNode()
  {
    return node;
  }

}