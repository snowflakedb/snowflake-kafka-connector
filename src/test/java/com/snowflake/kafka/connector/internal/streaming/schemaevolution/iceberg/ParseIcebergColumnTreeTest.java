package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.kafka.connector.records.RecordService;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.iceberg.types.Type;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ParseIcebergColumnTreeTest {

  @ParameterizedTest
  @MethodSource("icebergSchemas")
  void parseFromApacheIcebergSchema(String plainIcebergSchema, String expectedQuery) {
    // given
    Type type = IcebergDataTypeParser.deserializeIcebergType(plainIcebergSchema);
    // when
    ApacheIcebergColumnSchema apacheSchema =
        new ApacheIcebergColumnSchema(type, "TEST_COLUMN_NAME");
    IcebergColumnTree tree = new IcebergColumnTree(apacheSchema);
    // then
    Assertions.assertEquals(expectedQuery, tree.buildQueryPartWithNamesAndTypes());
  }

  static Stream<Arguments> icebergSchemas() {
    return Stream.of(
        // primitives
        arguments("\"boolean\"", "TEST_COLUMN_NAME BOOLEAN"),
        arguments("\"int\"", "TEST_COLUMN_NAME NUMBER(10,0)"),
        arguments("\"long\"", "TEST_COLUMN_NAME NUMBER(19,0)"),
        arguments("\"float\"", "TEST_COLUMN_NAME FLOAT"),
        arguments("\"double\"", "TEST_COLUMN_NAME FLOAT"),
        arguments("\"date\"", "TEST_COLUMN_NAME DATE"),
        arguments("\"time\"", "TEST_COLUMN_NAME TIME(6)"),
        arguments("\"timestamptz\"", "TEST_COLUMN_NAME TIMESTAMP_LTZ"),
        arguments("\"timestamp\"", "TEST_COLUMN_NAME TIMESTAMP"),
        arguments("\"string\"", "TEST_COLUMN_NAME VARCHAR(16777216)"),
        arguments("\"uuid\"", "TEST_COLUMN_NAME BINARY(16)"),
        arguments("\"binary\"", "TEST_COLUMN_NAME BINARY"),
        arguments("\"decimal(10,5)\"", "TEST_COLUMN_NAME DECIMAL(10, 5)"),
        // simple struct
        arguments(
            "{\"type\":\"struct\",\"fields\":[{\"id\":23,\"name\":\"k1\",\"required\":false,\"type\":\"int\"},{\"id\":24,\"name\":\"k2\",\"required\":false,\"type\":\"int\"}]}",
            "TEST_COLUMN_NAME OBJECT(k1 NUMBER(10,0), k2 NUMBER(10,0))"),
        // list
        arguments(
            "{\"type\":\"list\",\"element-id\":23,\"element\":\"long\",\"element-required\":false}",
            "TEST_COLUMN_NAME ARRAY(NUMBER(19,0))"),
        // map
        arguments(
            "{\"type\":\"map\",\"key-id\":4,\"key\":\"int\",\"value-id\":5,\"value\":\"string\",\"value-required\":false}",
            "TEST_COLUMN_NAME MAP(NUMBER(10,0), VARCHAR(16777216))"),
        // structs with nested objects
        arguments(
            "{\"type\":\"struct\",\"fields\":[{\"id\":23,\"name\":\"k1\",\"required\":false,\"type\":\"int\"},{\"id\":24,\"name\":\"k2\",\"required\":false,\"type\":\"int\"},{\"id\":25,\"name\":\"nested_object\",\"required\":false,\"type\":{\"type\":\"struct\",\"fields\":[{\"id\":26,\"name\":\"nested_key1\",\"required\":false,\"type\":\"string\"},{\"id\":27,\"name\":\"nested_key2\",\"required\":false,\"type\":\"string\"}]}}]}",
            "TEST_COLUMN_NAME OBJECT(k1 NUMBER(10,0), k2 NUMBER(10,0), nested_object"
                + " OBJECT(nested_key1 VARCHAR(16777216), nested_key2 VARCHAR(16777216)))"),
        arguments(
            "{\"type\":\"struct\",\"fields\":[{\"id\":2,\"name\":\"offset\",\"required\":false,\"type\":\"int\"},{\"id\":3,\"name\":\"topic\",\"required\":false,\"type\":\"string\"},{\"id\":4,\"name\":\"partition\",\"required\":false,\"type\":\"int\"},{\"id\":5,\"name\":\"key\",\"required\":false,\"type\":\"string\"},{\"id\":6,\"name\":\"schema_id\",\"required\":false,\"type\":\"int\"},{\"id\":7,\"name\":\"key_schema_id\",\"required\":false,\"type\":\"int\"},{\"id\":8,\"name\":\"CreateTime\",\"required\":false,\"type\":\"long\"},{\"id\":9,\"name\":\"LogAppendTime\",\"required\":false,\"type\":\"long\"},{\"id\":10,\"name\":\"SnowflakeConnectorPushTime\",\"required\":false,\"type\":\"long\"},{\"id\":11,\"name\":\"headers\",\"required\":false,\"type\":{\"type\":\"map\",\"key-id\":12,\"key\":\"string\",\"value-id\":13,\"value\":\"string\",\"value-required\":false}}]}\n",
            "TEST_COLUMN_NAME OBJECT(offset NUMBER(10,0), topic VARCHAR(16777216), partition"
                + " NUMBER(10,0), key VARCHAR(16777216), schema_id NUMBER(10,0), key_schema_id"
                + " NUMBER(10,0), CreateTime NUMBER(19,0), LogAppendTime NUMBER(19,0),"
                + " SnowflakeConnectorPushTime NUMBER(19,0), headers MAP(VARCHAR(16777216),"
                + " VARCHAR(16777216)))"));
  }

  @ParameterizedTest
  @MethodSource("recordNodes")
  void parseFromJsonRecordSchema(String jsonString, String expectedQuery) {
    // given
    SinkRecord record = createKafkaRecord(jsonString, false);
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    IcebergColumnJsonValuePair columnValuePair =
        IcebergColumnJsonValuePair.from(recordNode.fields().next());
    // when
    IcebergColumnTree tree = new IcebergColumnTree(columnValuePair);
    // then
    Assertions.assertEquals(expectedQuery, tree.buildQueryPartWithNamesAndTypes());
  }

  static Stream<Arguments> recordNodes() {
    return Stream.of(
        arguments("{\"test_number\" : 1 }", "test_number LONG"),
        arguments(
            "{ \"testStruct\": {" + "\"k1\" : 1," + "\"k2\" : 2" + "} " + "}",
            "testStruct OBJECT(k1 LONG, k2 LONG)"),
        arguments(
            "{ \"testStruct\": {"
                + "\"k1\" : { \"nested_key1\" : 1},"
                + "\"k2\" : { \"nested_key2\" : 2}"
                + "}}",
            "testStruct OBJECT(k1 OBJECT(nested_key1 LONG), k2 OBJECT(nested_key2 LONG))"),
        arguments(
            "{ \"vehiclesTestStruct\": {"
                + "\"vehicle1\" : { \"car\" : { \"brand\" : \"vw\" } },"
                + "\"vehicle2\" : { \"car\" : { \"brand\" : \"toyota\" } }"
                + "}}",
            "vehiclesTestStruct OBJECT(vehicle1 OBJECT(car OBJECT(brand VARCHAR)), vehicle2"
                + " OBJECT(car"
                + " OBJECT(brand"
                + " VARCHAR)))"), // todo lol przy k1, k2 normalna kolejnosc, a przy nazwach
        // vehicle1 i vehicle 2 juz inna
        arguments("{\"test_array\": [1,2,3] }", "not ready"));
  }

  protected SinkRecord createKafkaRecord(String jsonString, boolean withSchema) {
    int offset = 0;
    JsonConverter converter = new JsonConverter();
    converter.configure(
        Collections.singletonMap("schemas.enable", Boolean.toString(withSchema)), false);
    SchemaAndValue inputValue =
        converter.toConnectData("TOPIC_NAME", jsonString.getBytes(StandardCharsets.UTF_8));
    Headers headers = new ConnectHeaders();
    return new SinkRecord(
        "TOPIC_NAME",
        1,
        Schema.STRING_SCHEMA,
        "test",
        inputValue.schema(),
        inputValue.value(),
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME,
        headers);
  }
}