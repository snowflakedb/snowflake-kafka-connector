package com.snowflake.producer;

import com.snowflake.Utils;
import com.snowflake.schema.OneGTable;
import com.snowflake.test.Enums;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;

import java.util.Scanner;

public class OneGTableProducer extends  AvroProducer<OneGTable>
{
  @Override
  public void send(final Enums.TestCases testCase)
  {
    System.out.println("loading table: " + testCase.getTableName() +
      " in format: " + testCase.getFormatName() + " to Kafka");

    try
    {
      Scanner scanner = getFileScanner(testCase);
      while (scanner.hasNextLine())
      {
        JsonNode data = Utils.MAPPER.readTree(scanner.nextLine());
        send(Utils.TEST_TOPIC,
          OneGTable
            .newBuilder()
            .setCCUSTKEY(data.get("C_CUSTKEY").asLong())
            .setCNAME(data.get("C_NAME").asText())
            .setCADDRESS(data.get("C_ADDRESS").asText())
            .setCPHONE(data.get("C_PHONE").asText())
            .setCACCTBAL(data.get("C_ACCTBAL").asDouble())
            .setCMKTSEGMENT(data.get("C_MKTSEGMENT").asText())
            .setCCOMMENT(data.get("C_COMMENT").asText())
            .setCNATIONKEY(data.get("C_NATIONKEY").asLong())
            .build()
        );
      }
      scanner.close();
      close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("finished loading");
  }

}
