package com.snowflake.reporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.snowflake.Utils;
import com.snowflake.jdbc.ConnectionUtils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

public class FinalReport extends Reporter
{
  private final ObjectNode node;

  public FinalReport(JsonArray testSuites)
  {
    node = MAPPER.createObjectNode();
    node.put(CONNECTOR, getAppName());
    node.put(TIME, System.currentTimeMillis());
    node.set(TEST_SUITES, testSuites.getNode());

    System.out.println("create test report: " + toString());
  }

  public void output() throws IOException
  {
    File file = new File(Utils.REPORT_FILE_PATH);
    FileWriter writer = new FileWriter(file, false);
    writer.write(node.toString());
    writer.close();

    System.out.println("output report to " + Utils.REPORT_FILE_PATH);

    ConnectionUtils.uploadReport();
    ConnectionUtils.close();
  }

  @Override
  public JsonNode getNode()
  {
    return node;
  }
}