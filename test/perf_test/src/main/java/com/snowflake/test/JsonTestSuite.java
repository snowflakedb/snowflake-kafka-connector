package com.snowflake.test;

import com.snowflake.producer.JsonProducer;
import com.snowflake.producer.Producer;
import com.snowflake.test.Enums.Tables;
import com.snowflake.test.Enums.TestCases;

public class JsonTestSuite extends TestBase<String>
{
  private final JsonProducer producer;
  private final TestCases testCase;

  public JsonTestSuite(Tables table)
  {
    this.producer = new JsonProducer();
    switch (table)
    {
      case ONE_G_TABLE:
        this.testCase = TestCases.JSON_ONE_G_TABLE;
        break;
      case THREE_HUNDRED_COLUMN_TABLE:
        this.testCase = TestCases.JSON_THREE_HUNDRED_COLUMN_TABLE;
        break;
      default:
        this.testCase = null;
        System.err.println("Unsupport table name: " + table);
        System.exit(1);
    }

  }

  @Override
  protected Producer<String> getProducer()
  {
    return producer;
  }

  @Override
  protected TestCases getTestCase()
  {
    return testCase;
  }
}