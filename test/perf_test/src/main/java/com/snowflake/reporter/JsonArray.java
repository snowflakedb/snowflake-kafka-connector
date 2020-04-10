package com.snowflake.reporter;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ArrayNode;

public class JsonArray
{
  private ArrayNode node;

  public JsonArray()
  {
    node = Reporter.MAPPER.createArrayNode();
  }

  JsonArray(Reporter report)
  {
    this();
    node.add(report.getNode());
  }

  public ArrayNode getNode()
  {
    return node;
  }

  public JsonArray add(Reporter report)
  {
    node.add(report.getNode());
    return this;
  }

  @Override
  public String toString()
  {
    return node.toString();
  }
}