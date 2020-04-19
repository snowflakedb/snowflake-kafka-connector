package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.type.TypeReference;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RecordContentTest
{
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void test() throws IOException
  {
    JsonNode data = mapper.readTree("{\"name\":123}");
    //json
    SnowflakeRecordContent content =
      new SnowflakeRecordContent(data);
    assert !content.isBroken();
    assert content.getSchemaID() == SnowflakeRecordContent.NON_AVRO_SCHEMA;
    assert content.getData().length == 1;
    assert content.getData()[0].asText().equals(data.asText());
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5011,
      content::getBrokenData);

    //avro
    int schemaID = 123;
    content = new SnowflakeRecordContent(data, schemaID);
    assert !content.isBroken();
    assert content.getSchemaID() == schemaID;
    assert content.getData().length == 1;
    assert content.getData()[0].asText().equals(data.asText());
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5011,
      content::getBrokenData);

    //avro without schema registry
    JsonNode[] data1 = new JsonNode[1];
    data1[0] = data;
    content = new SnowflakeRecordContent(data1);
    assert !content.isBroken();
    assert content.getSchemaID() == SnowflakeRecordContent.NON_AVRO_SCHEMA;
    assert content.getData().length == 1;
    assert content.getData()[0].asText().equals(data.asText());
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5011,
      content::getBrokenData);

    //broken record
    byte[] brokenData = "123".getBytes(StandardCharsets.UTF_8);
    content = new SnowflakeRecordContent(brokenData);
    assert content.isBroken();
    assert content.getSchemaID() == SnowflakeRecordContent.NON_AVRO_SCHEMA;
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5012,
      content::getData);
    assert new String(content.getBrokenData(), StandardCharsets.UTF_8).equals("123");

    //null value
    content = new SnowflakeRecordContent();
    assert content.getData().length == 1;
    assert content.getData()[0].size() == 0;
    assert content.getData()[0].toString().equals("{}");

    // AVRO struct object
    SchemaBuilder builder = SchemaBuilder.struct()
      .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
      .field("int16", Schema.INT16_SCHEMA)
      .field("int32", Schema.INT32_SCHEMA)
      .field("int64", Schema.INT64_SCHEMA)
      .field("float32", Schema.FLOAT32_SCHEMA)
      .field("float64", Schema.FLOAT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("string", Schema.STRING_SCHEMA)
      .field("bytes", Schema.BYTES_SCHEMA)
      .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
      .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
      .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
        .build());
    Schema schema = builder.build();
    Struct original = new Struct(schema)
      .put("int8", (byte) 12)
      .put("int16", (short) 12)
      .put("int32", 12)
      .put("int64", 12L)
      .put("float32", 12.2f)
      .put("float64", 12.2)
      .put("boolean", true)
      .put("string", "foo")
      .put("bytes", ByteBuffer.wrap("foo".getBytes()))
      .put("array", Arrays.asList("a", "b", "c"))
      .put("map", Collections.singletonMap("field", 1))
      .put("mapNonStringKeys", Collections.singletonMap(1, 1));

    content = new SnowflakeRecordContent(schema, original);
    assert content.getData()[0].toString().equals("{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\",\"bytes\":\"Zm9v\",\"array\":[\"a\",\"b\",\"c\"],\"map\":{\"field\":1},\"mapNonStringKeys\":[[1,1]]}");

    // JSON map object
    JsonNode jsonObject = mapper.readTree("{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\",\"bytes\":\"Zm9v\",\"array\":[\"a\",\"b\",\"c\"],\"map\":{\"field\":1},\"mapNonStringKeys\":[[1,1]]}");
    Map<String, Object> jsonMap = mapper.convertValue(jsonObject, new TypeReference<Map<String, Object>>(){});
    content = new SnowflakeRecordContent(null, jsonMap);
    assert content.getData()[0].toString().equals("{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\",\"bytes\":\"Zm9v\",\"array\":[\"a\",\"b\",\"c\"],\"map\":{\"field\":1},\"mapNonStringKeys\":[[1,1]]}");
  }
}
