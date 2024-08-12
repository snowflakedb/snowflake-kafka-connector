package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

public class ValueSchemaTest {

  @Test
  public void testSnowflakeJsonSchema() {
    Schema schema = new SnowflakeJsonSchema();

    assertEquals(Schema.Type.STRUCT, schema.type());
    assertFalse(schema.isOptional());
    assertNull(schema.defaultValue());
    assertEquals(SnowflakeJsonSchema.NAME, schema.name());
    assertEquals(SnowflakeJsonSchema.VERSION, schema.version());
    assertNull(schema.doc());
    assertNull(schema.parameters());
    assertNull(schema.keySchema());
    assertNull(schema.valueSchema());
    assertNull(schema.fields());
    assertNull(schema.field(""));
    assertNull(schema.schema());
  }
}
