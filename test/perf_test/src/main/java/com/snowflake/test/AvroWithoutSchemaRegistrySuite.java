package com.snowflake.test;

import com.snowflake.producer.AvroWithoutSchemaRegistryProducer;
import com.snowflake.producer.Producer;

public class AvroWithoutSchemaRegistrySuite extends TestBase<byte[]>
{
  private final Enums.TestCases testCase;
  private final AvroWithoutSchemaRegistryProducer producer;

  public AvroWithoutSchemaRegistrySuite(Enums.Tables table)
  {
    this.producer = new AvroWithoutSchemaRegistryProducer();
    switch (table)
    {
      case ONE_G_TABLE:
        this.testCase = Enums.TestCases.AVRO_WITHOUT_SCHEMA_REGISTRY_ONE_G_TABLE;
        break;
      case THREE_HUNDRED_COLUMN_TABLE:
        this.testCase = Enums.TestCases.AVRO_WITHOUT_SCHEMA_REGISTRY_THREE_HUNDRED_COLUMN_TABLE;
        break;
      default:
        this.testCase = null;
        System.err.println("Unsupport table name: " + table);
        System.exit(1);
    }
  }

  @Override
  protected Producer<byte[]> getProducer()
  {
    return producer;
  }

  @Override
  protected Enums.TestCases getTestCase()
  {
    return testCase;
  }
}
