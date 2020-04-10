package com.snowflake.reporter;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

public class TestSuite extends Reporter
{
  private final ObjectNode node;

  public TestSuite(String name, JsonArray testCases)
  {
    node = MAPPER.createObjectNode();
    node.put(NAME, name);
    node.set(TEST_CASES, testCases.getNode());

    System.out.println("create test suite report: " + toString());
  }

  @Override
  public JsonNode getNode()
  {
    return node;
  }
}

