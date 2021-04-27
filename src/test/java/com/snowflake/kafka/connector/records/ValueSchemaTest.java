package com.snowflake.kafka.connector.records;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class ValueSchemaTest {
  @Test
  public void testSnowflakeJsonSchema() {
    Schema schema = new SnowflakeJsonSchema();

    assert schema.type() == Schema.Type.STRUCT;
    assert !schema.isOptional();
    assert schema.defaultValue() == null;
    assert schema.name().equals(SnowflakeJsonSchema.NAME);
    assert schema.version() == SnowflakeJsonSchema.VERSION;
    assert schema.doc() == null;
    assert schema.parameters() == null;
    assert schema.keySchema() == null;
    assert schema.valueSchema() == null;
    assert schema.fields() == null;
    assert schema.field("") == null;
    assert schema.schema() == null;
  }
}
