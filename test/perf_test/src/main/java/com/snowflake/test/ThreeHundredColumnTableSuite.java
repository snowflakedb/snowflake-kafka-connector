package com.snowflake.test;

import com.snowflake.producer.Producer;
import com.snowflake.producer.ThreeHundredColumnTableProducer;
import com.snowflake.schema.ThreeHundredColumnTable;

public class ThreeHundredColumnTableSuite extends AvroTestSuite<ThreeHundredColumnTable>
{
  @Override
  protected Producer<ThreeHundredColumnTable> getProducer()
  {
    return new ThreeHundredColumnTableProducer();
  }

  @Override
  protected Enums.TestCases getTestCase()
  {
    return Enums.TestCases.AVRO_WITH_SCHEMA_REGISTRY_THREE_HUNDRED_COLUMN_TABLE;
  }
}
