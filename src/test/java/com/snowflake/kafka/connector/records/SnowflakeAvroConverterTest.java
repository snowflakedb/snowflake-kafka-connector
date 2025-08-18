/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeAvroConverterTest {

  private SnowflakeAvroConverter converter;

  @BeforeEach
  public void setUp() {
    converter = new SnowflakeAvroConverter();
  }

  @Test
  public void testParseAvroWithSchema_ConnectTypeProperties() throws Exception {
    // Create a schema with connect.type properties
    String schemaJson = "{"
        + "\"name\":\"TestRecord\","
        + "\"type\":\"record\","
        + "\"fields\":["
        + "  {\"name\":\"stringField\",\"type\":\"string\",\"connect.type\":\"string\"},"
        + "  {\"name\":\"intField\",\"type\":\"int\",\"connect.type\":\"int32\"},"
        + "  {\"name\":\"longField\",\"type\":\"long\",\"connect.type\":\"int64\"},"
        + "  {\"name\":\"floatField\",\"type\":\"float\",\"connect.type\":\"float32\"},"
        + "  {\"name\":\"doubleField\",\"type\":\"double\",\"connect.type\":\"float64\"},"
        + "  {\"name\":\"booleanField\",\"type\":\"boolean\",\"connect.type\":\"boolean\"}"
        + "]"
        + "}";

    Schema schema = new Schema.Parser().parse(schemaJson);

    // Create test data
    GenericRecord record = new GenericData.Record(schema);
    record.put("stringField", "test_value");
    record.put("intField", 42);
    record.put("longField", 1234567890L);
    record.put("floatField", 3.14f);
    record.put("doubleField", 2.718281828);
    record.put("booleanField", true);

    // Serialize the record to bytes
    byte[] avroData = serializeAvroRecord(record, schema);

    // Use reflection to call the private parseAvroWithSchema method
    Method parseMethod = SnowflakeAvroConverter.class.getDeclaredMethod(
        "parseAvroWithSchema", byte[].class, Schema.class, Schema.class);
    parseMethod.setAccessible(true);

//    return new SchemaAndValue(
//          new SnowflakeJsonSchema(),
//          new SnowflakeRecordContent(
//              parseAvroWithSchema(
//                  data, writerSchema, readerSchema == null ? writerSchema : readerSchema),
//              id));

    // Test with writer and reader schema being the same
    JsonNode result = (JsonNode) parseMethod.invoke(converter, avroData, schema, schema);

    SchemaAndValue x = new SchemaAndValue(
        new SnowflakeJsonSchema(),
        new SnowflakeRecordContent(result)
    );

    assertNotNull(result);
    assertEquals("test_value", result.get("stringField").asText());
    assertEquals(42, result.get("intField").asInt());
    assertEquals(1234567890L, result.get("longField").asLong());
    assertEquals(3.14f, result.get("floatField").asDouble(), 0.001);
    assertEquals(2.718281828, result.get("doubleField").asDouble(), 0.000001);
    assertTrue(result.get("booleanField").asBoolean());
  }

  @Test
  public void testParseAvroWithSchema_SchemaEvolution() throws Exception {
    // Create writer schema
    String writerSchemaJson = "{"
        + "\"name\":\"TestRecord\","
        + "\"type\":\"record\","
        + "\"fields\":["
        + "  {\"name\":\"stringField\",\"type\":\"string\",\"connect.type\":\"string\"},"
        + "  {\"name\":\"intField\",\"type\":\"int\",\"connect.type\":\"int32\"}"
        + "]"
        + "}";

    // Create reader schema with additional field
    String readerSchemaJson = "{"
        + "\"name\":\"TestRecord\","
        + "\"type\":\"record\","
        + "\"fields\":["
        + "  {\"name\":\"stringField\",\"type\":\"string\",\"connect.type\":\"string\"},"
        + "  {\"name\":\"intField\",\"type\":\"int\",\"connect.type\":\"int32\"},"
        + "  {\"name\":\"newField\",\"type\":\"string\",\"connect.type\":\"string\",\"default\":\"default_value\"}"
        + "]"
        + "}";

    Schema writerSchema = new Schema.Parser().parse(writerSchemaJson);
    Schema readerSchema = new Schema.Parser().parse(readerSchemaJson);

    // Create test data with writer schema
    GenericRecord record = new GenericData.Record(writerSchema);
    record.put("stringField", "test_value");
    record.put("intField", 123);

    // Serialize the record to bytes
    byte[] avroData = serializeAvroRecord(record, writerSchema);

    // Use reflection to call the private parseAvroWithSchema method
    Method parseMethod = SnowflakeAvroConverter.class.getDeclaredMethod(
        "parseAvroWithSchema", byte[].class, Schema.class, Schema.class);
    parseMethod.setAccessible(true);

    // Test with different writer and reader schemas
    JsonNode result = (JsonNode) parseMethod.invoke(converter, avroData, writerSchema, readerSchema);

    assertNotNull(result);
    assertEquals("test_value", result.get("stringField").asText());
    assertEquals(123, result.get("intField").asInt());
    assertEquals("default_value", result.get("newField").asText());
  }

  @Test
  public void testParseAvroWithSchema_LogicalTypes() throws Exception {
    // Create a schema with logical types and connect.type properties
    String schemaJson = "{"
        + "\"name\":\"TestRecord\","
        + "\"type\":\"record\","
        + "\"fields\":["
        + "  {\"name\":\"timestampField\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"connect.type\":\"timestamp\"},"
        + "  {\"name\":\"dateField\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"},\"connect.type\":\"date\"}"
        + "]"
        + "}";

    Schema schema = new Schema.Parser().parse(schemaJson);

    // Create test data
    GenericRecord record = new GenericData.Record(schema);

    // For timestamp-millis, use current time in milliseconds
    long timestamp = 1609459200000L; // 2021-01-01 00:00:00 UTC
    record.put("timestampField", timestamp);

    // For date, use days since epoch
    int date = 18628; // 2021-01-01
    record.put("dateField", date);

    // Serialize the record to bytes
    byte[] avroData = serializeAvroRecord(record, schema);

    // Use reflection to call the private parseAvroWithSchema method
    Method parseMethod = SnowflakeAvroConverter.class.getDeclaredMethod(
        "parseAvroWithSchema", byte[].class, Schema.class, Schema.class);
    parseMethod.setAccessible(true);

    JsonNode result = (JsonNode) parseMethod.invoke(converter, avroData, schema, schema);

    assertNotNull(result);
    // Verify the fields are present (exact values may vary based on logical type handling)
    assertTrue(result.has("timestampField"));
    assertTrue(result.has("dateField"));
  }

  @Test
  public void testParseAvroWithSchema_NestedRecord() throws Exception {
    // Create a schema with nested records and connect.type properties
    String nestedSchemaJson = "{"
        + "\"name\":\"NestedRecord\","
        + "\"type\":\"record\","
        + "\"fields\":["
        + "  {\"name\":\"nestedString\",\"type\":\"string\",\"connect.type\":\"string\"},"
        + "  {\"name\":\"nestedInt\",\"type\":\"int\",\"connect.type\":\"int32\"}"
        + "]"
        + "}";

    String mainSchemaJson = "{"
        + "\"name\":\"MainRecord\","
        + "\"type\":\"record\","
        + "\"fields\":["
        + "  {\"name\":\"mainField\",\"type\":\"string\",\"connect.type\":\"string\"},"
        + "  {\"name\":\"nestedField\",\"type\":" + nestedSchemaJson + ",\"connect.type\":\"struct\"}"
        + "]"
        + "}";

    Schema mainSchema = new Schema.Parser().parse(mainSchemaJson);
    Schema nestedSchema = mainSchema.getField("nestedField").schema();

    // Create test data
    GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
    nestedRecord.put("nestedString", "nested_value");
    nestedRecord.put("nestedInt", 456);

    GenericRecord mainRecord = new GenericData.Record(mainSchema);
    mainRecord.put("mainField", "main_value");
    mainRecord.put("nestedField", nestedRecord);

    // Serialize the record to bytes
    byte[] avroData = serializeAvroRecord(mainRecord, mainSchema);

    // Use reflection to call the private parseAvroWithSchema method
    Method parseMethod = SnowflakeAvroConverter.class.getDeclaredMethod(
        "parseAvroWithSchema", byte[].class, Schema.class, Schema.class);
    parseMethod.setAccessible(true);

    JsonNode result = (JsonNode) parseMethod.invoke(converter, avroData, mainSchema, mainSchema);

    assertNotNull(result);
    assertEquals("main_value", result.get("mainField").asText());

    JsonNode nestedResult = result.get("nestedField");
    assertNotNull(nestedResult);
    assertEquals("nested_value", nestedResult.get("nestedString").asText());
    assertEquals(456, nestedResult.get("nestedInt").asInt());
  }

  @Test
  public void testParseAvroWithSchema_ArrayType() throws Exception {
    // Create a schema with array type and connect.type properties
    String schemaJson = "{"
        + "\"name\":\"TestRecord\","
        + "\"type\":\"record\","
        + "\"fields\":["
        + "  {\"name\":\"stringArray\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"connect.type\":\"array\"},"
        + "  {\"name\":\"intArray\",\"type\":{\"type\":\"array\",\"items\":\"int\"},\"connect.type\":\"array\"}"
        + "]"
        + "}";

    Schema schema = new Schema.Parser().parse(schemaJson);

    // Create test data
    GenericRecord record = new GenericData.Record(schema);
    record.put("stringArray", Arrays.asList("item1", "item2", "item3"));
    record.put("intArray", Arrays.asList(1, 2, 3, 4, 5));

    // Serialize the record to bytes
    byte[] avroData = serializeAvroRecord(record, schema);

    // Use reflection to call the private parseAvroWithSchema method
    Method parseMethod = SnowflakeAvroConverter.class.getDeclaredMethod(
        "parseAvroWithSchema", byte[].class, Schema.class, Schema.class);
    parseMethod.setAccessible(true);

    JsonNode result = (JsonNode) parseMethod.invoke(converter, avroData, schema, schema);

    assertNotNull(result);

    JsonNode stringArrayResult = result.get("stringArray");
    assertNotNull(stringArrayResult);
    assertTrue(stringArrayResult.isArray());
    assertEquals(3, stringArrayResult.size());
    assertEquals("item1", stringArrayResult.get(0).asText());
    assertEquals("item2", stringArrayResult.get(1).asText());
    assertEquals("item3", stringArrayResult.get(2).asText());

    JsonNode intArrayResult = result.get("intArray");
    assertNotNull(intArrayResult);
    assertTrue(intArrayResult.isArray());
    assertEquals(5, intArrayResult.size());
    assertEquals(1, intArrayResult.get(0).asInt());
    assertEquals(5, intArrayResult.get(4).asInt());
  }

  @Test
  public void testFailingProdExample() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    String schemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Value\",\n" +
            "  \"namespace\": \"leap-tenant-dev4.pg-cc.v4.public.accounts_user\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"tenant\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"password\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"last_login\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"string\",\n" +
            "          \"connect.version\": 1,\n" +
            "          \"connect.name\": \"io.debezium.time.ZonedTimestamp\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"is_superuser\",\n" +
            "      \"type\": \"boolean\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"username\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"first_name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"last_name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"is_staff\",\n" +
            "      \"type\": \"boolean\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"is_active\",\n" +
            "      \"type\": \"boolean\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"date_joined\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"connect.version\": 1,\n" +
            "        \"connect.name\": \"io.debezium.time.ZonedTimestamp\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"created\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"connect.version\": 1,\n" +
            "        \"connect.name\": \"io.debezium.time.ZonedTimestamp\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"modified\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"connect.version\": 1,\n" +
            "        \"connect.name\": \"io.debezium.time.ZonedTimestamp\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"created_by_username\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"created_by_name\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"modified_by_username\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"modified_by_name\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"removed\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"string\",\n" +
            "          \"connect.version\": 1,\n" +
            "          \"connect.name\": \"io.debezium.time.ZonedTimestamp\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"uuid\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"connect.version\": 1,\n" +
            "        \"connect.name\": \"io.debezium.data.Uuid\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"email\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"metadata\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"connect.version\": 1,\n" +
            "        \"connect.name\": \"io.debezium.data.Json\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"performance_logging\",\n" +
            "      \"type\": \"boolean\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"status\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"phone_number\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"address\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"preferred_name\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"act_as_regular_user\",\n" +
            "      \"type\": \"boolean\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"tenant_id\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"string\",\n" +
            "          \"connect.version\": 1,\n" +
            "          \"connect.name\": \"io.debezium.data.Uuid\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"permission_logging\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"country\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"date_of_birth\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"date_of_hire\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"department\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"employment_type\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"gender\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"role\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"title\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"user_type\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    Schema schema = new Schema.Parser().parse(schemaJson);

    // Create test data
    GenericRecord record = new GenericData.Record(schema);
    record.put("tenant", "test_tenant");
    record.put("password", "test_password");
    record.put("last_login", null); // Null value for ZonedTimestamp
    record.put("is_superuser", false);
    record.put("username", "test_user");
    record.put("first_name", "Test");
    record.put("last_name", "User");
    record.put("is_staff", true);
    record.put("is_active", true);
    record.put("date_joined", "2021-01-01T00:00:00Z"); // ZonedTimestamp as string
    record.put("created", "2021-01-01T00:00:00Z"); // ZonedTimestamp as string
    record.put("modified", "2021-01-01T00:00:00Z"); // ZonedTimestamp as string
    record.put("created_by_username", null); // Null value
    record.put("created_by_name", null); // Null value
    record.put("modified_by_username", null); // Null value
    record.put("modified_by_name", null); // Null value
    record.put("removed", null); // Null value for ZonedTimestamp
    record.put("uuid", "123e4567-e89b-12d3-a456-426614174000"); // UUID as string
    record.put("email", "test@gmail.com");
    record.put("metadata", "{\"key\":\"value\"}"); // JSON as string
    record.put("performance_logging", true);
    record.put("status", null); // Null value
    record.put("phone_number", null); // Null value
    record.put("address", null); // Null value
    record.put("preferred_name", null); // Null value
    record.put("act_as_regular_user", false);
    record.put("tenant_id", null); // Null value for UUID
    record.put("permission_logging", "enabled");
    record.put("country", null); // Null value
    record.put("date_of_birth", null); // Null value
    record.put("date_of_hire", null); // Null value
    record.put("department", null); // Null value
    record.put("employment_type", null); // Null value
    record.put("gender", null);
    record.put("role", null); // Null value
    record.put("title", null); // Null value
    record.put("user_type", null); // Null value

    // FIXME: Assert date/time types parsed by "getFullSchemaMapFromRecord" are what we expect
    // Serialize the record to bytes
    byte[] avroData = serializeAvroRecord(record, schema);
    // Use reflection to call the private getFullSchemaMapFromRecord method
    Method getSchemaMethod = SnowflakeAvroConverter.class.getDeclaredMethod(
        "getFullSchemaMapFromRecord", byte[].class, Schema.class, Schema.class);
    getSchemaMethod.setAccessible(true);


  }

  /**
   * Helper method to serialize an Avro record to bytes
   */
  private byte[] serializeAvroRecord(GenericRecord record, Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
