package com.snowflake;

import com.snowflake.reporter.FinalReport;
import com.snowflake.test.AvroWithoutSchemaRegistrySuite;
import com.snowflake.test.Enums;
import com.snowflake.test.JsonTestSuite;
import com.snowflake.test.OneGTableSuite;
import com.snowflake.test.ThreeHundredColumnTableSuite;
import org.junit.Test;

import java.io.IOException;

public class PerfTest
{
  @Test
  public void test() throws IOException
  {
    (new FinalReport(
      (new JsonTestSuite(Enums.Tables.ONE_G_TABLE)).run().add(
        (new JsonTestSuite(Enums.Tables.THREE_HUNDRED_COLUMN_TABLE)).run()).add(
        (new OneGTableSuite()).run()).add(
        (new ThreeHundredColumnTableSuite()).run()).add(
        (new AvroWithoutSchemaRegistrySuite(Enums.Tables.ONE_G_TABLE)).run()).add(
        (new AvroWithoutSchemaRegistrySuite(Enums.Tables.THREE_HUNDRED_COLUMN_TABLE).run()))
    )).output();
  }
}
