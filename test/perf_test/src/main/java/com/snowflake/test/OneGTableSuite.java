package com.snowflake.test;

import com.snowflake.producer.OneGTableProducer;
import com.snowflake.producer.Producer;
import com.snowflake.schema.OneGTable;

public class OneGTableSuite extends AvroTestSuite<OneGTable>
{
  @Override
  protected Producer<OneGTable> getProducer()
  {
    return new OneGTableProducer();
  }

  @Override
  protected Enums.TestCases getTestCase()
  {
    return Enums.TestCases.AVRO_WITH_SCHEMA_REGISTRY_ONE_G_TABLE;
  }
}
