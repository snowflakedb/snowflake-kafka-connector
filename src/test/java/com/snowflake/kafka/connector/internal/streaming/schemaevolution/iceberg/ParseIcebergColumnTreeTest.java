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

  private final IcebergColumnTreeFactory treeFactory = new IcebergColumnTreeFactory();
  private final IcebergColumnTreeMerger mergeTreeService = new IcebergColumnTreeMerger();
  private final IcebergColumnTreeTypeBuilder typeBuilder = new IcebergColumnTreeTypeBuilder();

  @ParameterizedTest
  @MethodSource("icebergSchemas")
  void parseFromApacheIcebergSchema(String plainIcebergSchema, String expectedType) {
    // given
    Type type = IcebergDataTypeParser.deserializeIcebergType(plainIcebergSchema);
    // when
    IcebergColumnSchema apacheSchema = new IcebergColumnSchema(type, "TEST_COLUMN_NAME");
    IcebergColumnTree tree = treeFactory.fromIcebergSchema(apacheSchema);
    // then
    Assertions.assertEquals(expectedType, typeBuilder.buildType(tree));
    Assertions.assertEquals("TEST_COLUMN_NAME", tree.getColumnName());
  }

  static Stream<Arguments> icebergSchemas() {
    return Stream.of(
        // primitives
        arguments("\"boolean\"", "BOOLEAN"),
        arguments("\"int\"", "INT"),
        arguments("\"long\"", "LONG"),
        arguments("\"float\"", "DOUBLE"),
        arguments("\"double\"", "DOUBLE"),
        arguments("\"date\"", "DATE"),
        arguments("\"time\"", "TIME(6)"),
        arguments("\"timestamptz\"", "TIMESTAMP_LTZ"),
        arguments("\"timestamp\"", "TIMESTAMP"),
        arguments("\"string\"", "STRING"),
        arguments("\"uuid\"", "BINARY(16)"),
        arguments("\"binary\"", "BINARY"),
        arguments("\"decimal(10,5)\"", "DECIMAL(10, 5)"),
        // simple struct
        arguments(
            "{\"type\":\"struct\",\"fields\":[{\"id\":23,\"name\":\"k1\",\"required\":false,\"type\":\"int\"},{\"id\":24,\"name\":\"k2\",\"required\":false,\"type\":\"int\"}]}",
            "OBJECT(k1 INT, k2 INT)"),
        // list
        arguments(
            "{\"type\":\"list\",\"element-id\":23,\"element\":\"long\",\"element-required\":false}",
            "ARRAY(LONG)"),
        arguments(
            "{\"type\":\"list\",\"element-id\":1,\"element\":{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"primitive\",\"required\":true,\"type\":\"boolean\"}]},\"element-required\":true}",
            "ARRAY(OBJECT(primitive BOOLEAN))"),
        // map
        arguments(
            "{\"type\":\"map\",\"key-id\":4,\"key\":\"int\",\"value-id\":5,\"value\":\"string\",\"value-required\":false}",
            "MAP(INT, STRING)"),
        // structs with nested objects
        arguments(
            "{\"type\":\"struct\",\"fields\":["
                + "{\"id\":23,\"name\":\"k1\",\"required\":false,\"type\":\"int\"},{\"id\":24,\"name\":\"k2\",\"required\":false,\"type\":\"int\"},"
                + "  {\"id\":25,\"name\":\"nested_object\",\"required\":false,\"type\":{\"type\":\"struct\",\"fields\":["
                + "    {\"id\":26,\"name\":\"nested_key1\",\"required\":false,\"type\":\"string\"},"
                + "    {\"id\":27,\"name\":\"nested_key2\",\"required\":false,\"type\":\"string\"}"
                + "]}}]}",
            "OBJECT(k1 INT, k2 INT, nested_object"
                + " OBJECT(nested_key1 STRING, nested_key2 STRING))"),
        arguments(
            "{\"type\":\"struct\",\"fields\":[{\"id\":2,\"name\":\"offset\",\"required\":false,\"type\":\"int\"},{\"id\":3,\"name\":\"topic\",\"required\":false,\"type\":\"string\"},{\"id\":4,\"name\":\"partition\",\"required\":false,\"type\":\"int\"},{\"id\":5,\"name\":\"key\",\"required\":false,\"type\":\"string\"},{\"id\":6,\"name\":\"schema_id\",\"required\":false,\"type\":\"int\"},{\"id\":7,\"name\":\"key_schema_id\",\"required\":false,\"type\":\"int\"},{\"id\":8,\"name\":\"CreateTime\",\"required\":false,\"type\":\"long\"},{\"id\":9,\"name\":\"LogAppendTime\",\"required\":false,\"type\":\"long\"},{\"id\":10,\"name\":\"SnowflakeConnectorPushTime\",\"required\":false,\"type\":\"long\"},{\"id\":11,\"name\":\"headers\",\"required\":false,\"type\":{\"type\":\"map\",\"key-id\":12,\"key\":\"string\",\"value-id\":13,\"value\":\"string\",\"value-required\":false}}]}\n",
            "OBJECT(offset INT, topic STRING, partition"
                + " INT, key STRING, schema_id INT, key_schema_id"
                + " INT, CreateTime LONG, LogAppendTime LONG,"
                + " SnowflakeConnectorPushTime LONG, headers MAP(STRING,"
                + " STRING))"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("parseFromJsonArguments")
  void parseFromJsonRecordSchema(String jsonString, String expectedType) {
    // given
    SinkRecord record = createKafkaRecord(jsonString);
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    IcebergColumnJsonValuePair columnValuePair =
        IcebergColumnJsonValuePair.from(recordNode.fields().next());
    // when
    IcebergColumnTree tree = treeFactory.fromJson(columnValuePair);
    // then
    Assertions.assertEquals(expectedType, typeBuilder.buildType(tree));
    Assertions.assertEquals("TESTCOLUMNNAME", tree.getColumnName());
  }

  static Stream<Arguments> parseFromJsonArguments() {
    return Stream.of(
        arguments("{\"testColumnName\" : 1 }", "LONG"),
        arguments(
            "{ \"testColumnName\": { \"k1\" : 1, \"k2\" : 2 }  }", "OBJECT(k1 LONG, k2 LONG)"),
        arguments(
            "{ \"testColumnName\": {"
                + "\"k1\" : { \"nested_key1\" : 1},"
                + "\"k2\" : { \"nested_key2\" : 2}"
                + "}}",
            "OBJECT(k1 OBJECT(nested_key1 LONG), k2 OBJECT(nested_key2 LONG))"),
        arguments(
            "{ \"testColumnName\": {"
                + "\"vehicle1\" : { \"car\" : { \"brand\" : \"vw\" } },"
                + "\"vehicle2\" : { \"car\" : { \"brand\" : \"toyota\" } }"
                + "}}",
            "OBJECT(vehicle2 OBJECT(car OBJECT(brand VARCHAR)), "
                + "vehicle1 OBJECT(car OBJECT(brand VARCHAR)))"),
        arguments(
            "{ \"testColumnName\": {"
                + "\"k1\" : { \"car\" : { \"brand\" : \"vw\" } },"
                + "\"k2\" : { \"car\" : { \"brand\" : \"toyota\" } }"
                + "}}",
            "OBJECT(k1 OBJECT(car OBJECT(brand VARCHAR)), k2"
                + " OBJECT(car"
                + " OBJECT(brand"
                + " VARCHAR)))"),
        arguments(
            "   { \"testColumnName\": ["
                + "      {"
                + "        \"id\": 0,"
                + "        \"name\": \"Sandoval Hodges\""
                + "      },"
                + "      {"
                + "        \"id\": 1,"
                + "        \"name\": \"Ramirez Brooks\""
                + "      },"
                + "      {"
                + "        \"id\": 2,"
                + "        \"name\": \"Vivian Whitfield\""
                + "      }"
                + "    ] } ",
            "ARRAY(OBJECT(name VARCHAR, id LONG))"),
        // array
        arguments("{\"testColumnName\": [1,2,3] }", "ARRAY(LONG)"));
  }

  @ParameterizedTest
  @MethodSource("mergeTestArguments")
  void mergeTwoTreesTest(String plainIcebergSchema, String recordJson, String expectedResult) {
    // given tree parsed from channel
    Type type = IcebergDataTypeParser.deserializeIcebergType(plainIcebergSchema);
    IcebergColumnSchema apacheSchema = new IcebergColumnSchema(type, "TESTSTRUCT");
    IcebergColumnTree alreadyExistingTree = treeFactory.fromIcebergSchema(apacheSchema);

    // tree parsed from a record
    SinkRecord record = createKafkaRecord(recordJson);
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    IcebergColumnJsonValuePair columnValuePair =
        IcebergColumnJsonValuePair.from(recordNode.fields().next());

    IcebergColumnTree modifiedTree = treeFactory.fromJson(columnValuePair);
    // when
    mergeTreeService.merge(alreadyExistingTree, modifiedTree);
    // then
    String expected = expectedResult.replaceAll("/ +/g", " ");
    Assertions.assertEquals(expected, typeBuilder.buildType(alreadyExistingTree));
    Assertions.assertEquals("TESTSTRUCT", alreadyExistingTree.getColumnName());
  }

  static Stream<Arguments> mergeTestArguments() {
    return Stream.of(
        arguments(
            "{\"type\":\"struct\",\"fields\":["
                + "{\"id\":23,\"name\":\"k1\",\"required\":false,\"type\":\"int\"},"
                + "{\"id\":24,\"name\":\"k2\",\"required\":false,\"type\":\"int\"}"
                + "]}",
            "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2, \"k3\" : 3 } }",
            "OBJECT(k1 INT, k2 INT, k3 LONG)"),
        arguments(
            "{\"type\":\"struct\",\"fields\":["
                + "{\"id\":23,\"name\":\"k1\",\"required\":false,\"type\":\"int\"},{\"id\":24,\"name\":\"k2\",\"required\":false,\"type\":\"int\"},"
                + "  {\"id\":25,\"name\":\"nested_object\",\"required\":false,\"type\":{\"type\":\"struct\",\"fields\":["
                + "    {\"id\":26,\"name\":\"nested_key1\",\"required\":false,\"type\":\"string\"},"
                + "    {\"id\":27,\"name\":\"nested_key2\",\"required\":false,\"type\":\"string\"}"
                + "]}}]}",
            "{\"testStruct\" : {"
                + "  \"k1\" : 1, "
                + "  \"k2\" : 2, "
                + "    \"nested_object\": { "
                + "      \"nested_key1\" : \"string\", "
                + "      \"nested_key2\" : \"blah\", "
                + "      \"nested_object2\" : { "
                + "        \"nested_key2\" : 23.5 "
                + "    }}"
                + "}}",
            "OBJECT(k1 INT, k2 INT, nested_object OBJECT(nested_key1"
                + " STRING, nested_key2 STRING, nested_object2"
                + " OBJECT(nested_key2 DOUBLE)))"),
        // ARRAY merge
        arguments(
            "{\"type\":\"list\",\"element-id\":23,\"element\":\"long\",\"element-required\":false}",
            "{\"TESTSTRUCT\": [1,2,3] }",
            "ARRAY(LONG)"),
        arguments(
            "{\"type\":\"list\",\"element-id\":1,\"element\":{"
                + "\"type\":\"struct\",\"fields\":["
                + "{\"id\":1,\"name\":\"primitive\",\"required\":true,\"type\":\"boolean\"}"
                + "]},"
                + "\"element-required\":true}",
            "{\"TESTSTRUCT\": [ { \"primitive\" : true, \"new_field\" : 25878749237493287429348 }]"
                + " }",
            "ARRAY(OBJECT(primitive BOOLEAN, new_field LONG))"));
  }

  protected SinkRecord createKafkaRecord(String jsonString) {
    int offset = 0;
    JsonConverter converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", Boolean.toString(false)), false);
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
