package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC;
import static com.snowflake.kafka.connector.records.RecordService.KEY;
import static com.snowflake.kafka.connector.records.RecordService.OFFSET;
import static com.snowflake.kafka.connector.records.RecordService.PARTITION;
import static com.snowflake.kafka.connector.records.RecordService.TOPIC;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.mock.MockSchemaRegistryClient;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

abstract class AbstractMetaColumnTest {

  private static final String TEST_VALUE_FILE_NAME = "test.avro";

  protected final String topic = "test";
  protected final int partition = 0;

  /**
   * Expected to be implemented by concrete Snowpipe/Snowpipe Streaming tests.
   *
   * @param service {@link RecordService} under test.
   * @param record {@link SinkRecord} which metadata is requested.
   * @return a nullable {@link JsonNode} that represents record metadata.
   * @throws JsonProcessingException if json deserialization error occurs.
   */
  abstract @Nullable JsonNode getMetadataNode(RecordService service, SinkRecord record)
      throws JsonProcessingException;

  @Test
  public void testKey() throws IOException {
    RecordService service = RecordServiceFactory.createRecordService(false, false);
    SchemaAndValue input = getJsonInputData();
    long timestamp = System.currentTimeMillis();

    // no timestamp
    SinkRecord record =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            input.schema(),
            input.value(),
            0,
            timestamp,
            TimestampType.NO_TIMESTAMP_TYPE);

    JsonNode metadata = getMetadataNode(service, record);

    assertNotNull(metadata);
    assertTrue(metadata.has(KEY));
    assertEquals("test", metadata.get(KEY).asText());
  }

  @ParameterizedTest
  @MethodSource("dataFor_disablingFields")
  void whenSomeFieldsDisabled(Map<String, String> config, Set<String> enabledFields)
      throws JsonProcessingException {
    // given
    TimestampType timestampType = TimestampType.CREATE_TIME;

    SchemaAndValue input = getJsonInputData();
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition)
            .withKeySchema(Schema.STRING_SCHEMA)
            .withKey("test")
            .withValueSchema(input.schema())
            .withValue(input.value())
            .withOffset(0)
            .withTimestamp(System.currentTimeMillis(), timestampType)
            .build();

    RecordService service = RecordServiceFactory.createRecordService(false, false);
    service.setMetadataConfig(new SnowflakeMetadataConfig(config));

    // when
    JsonNode metadata = getMetadataNode(service, record);

    // then
    assertNotNull(metadata);
    assertEquals(enabledFields.contains(TOPIC), metadata.has(TOPIC));
    assertEquals(enabledFields.contains(OFFSET), metadata.has(OFFSET));
    assertEquals(enabledFields.contains(PARTITION), metadata.has(PARTITION));
    assertEquals(enabledFields.contains(timestampType.name), metadata.has(timestampType.name));
    // ConnectorPushTime is present for SnowpipeStreaming only.
  }

  static Stream<Arguments> dataFor_disablingFields() {
    return Stream.of(
        arguments(
            ImmutableMap.of(SNOWFLAKE_METADATA_CREATETIME, "false"),
            ImmutableSet.of(TOPIC, PARTITION, OFFSET)),
        arguments(
            ImmutableMap.of(SNOWFLAKE_METADATA_TOPIC, "false"),
            ImmutableSet.of(PARTITION, OFFSET, TimestampType.CREATE_TIME.name)),
        arguments(
            ImmutableMap.of(SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "false"),
            ImmutableSet.of(TOPIC, TimestampType.CREATE_TIME.name)),
        arguments(
            ImmutableMap.of(
                SNOWFLAKE_METADATA_CREATETIME,
                "false",
                SNOWFLAKE_METADATA_TOPIC,
                "false",
                SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
                "false"),
            emptySet()));
  }

  @Test
  void whenMetadataDisabled() throws IOException {
    // given
    SchemaAndValue input = getJsonInputData();
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition)
            .withValueSchema(input.schema())
            .withValue(input.value())
            .build();

    Map<String, String> config = ImmutableMap.of(SNOWFLAKE_METADATA_ALL, "false");

    RecordService service = RecordServiceFactory.createRecordService(false, false);
    service.setMetadataConfig(new SnowflakeMetadataConfig(config));

    // when
    JsonNode metadata = getMetadataNode(service, record);

    // then
    assertNull(metadata);
  }

  @Test
  public void testTimeStamp() throws IOException {
    RecordService service = RecordServiceFactory.createRecordService(false, false);
    SchemaAndValue input = getJsonInputData();
    long timestamp = System.currentTimeMillis();

    // no timestamp
    SinkRecord record =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            input.schema(),
            input.value(),
            0,
            timestamp,
            TimestampType.NO_TIMESTAMP_TYPE);

    JsonNode metadata = getMetadataNode(service, record);

    assertNotNull(metadata);
    assertFalse(metadata.has(TimestampType.CREATE_TIME.name));
    assertFalse(metadata.has(TimestampType.LOG_APPEND_TIME.name));

    // create time
    record =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            input.schema(),
            input.value(),
            0,
            timestamp,
            TimestampType.CREATE_TIME);

    metadata = getMetadataNode(service, record);

    assertNotNull(metadata);
    assertTrue(metadata.has(TimestampType.CREATE_TIME.name));
    assertEquals(timestamp, metadata.get(TimestampType.CREATE_TIME.name).asLong());

    // log append time
    record =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            input.schema(),
            input.value(),
            0,
            timestamp,
            TimestampType.LOG_APPEND_TIME);

    metadata = getMetadataNode(service, record);

    assertNotNull(metadata);
    assertTrue(metadata.has(TimestampType.LOG_APPEND_TIME.name));
    assertEquals(timestamp, metadata.get(TimestampType.LOG_APPEND_TIME.name).asLong());
  }

  @Test
  public void testSchemaID() throws IOException {
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, ("{\"name\":\"test\"}").getBytes(StandardCharsets.UTF_8));

    // no schema id
    SinkRecord record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), 0);
    SnowflakeRecordContent content = assertInstanceOf(SnowflakeRecordContent.class, record.value());

    assertEquals(SnowflakeRecordContent.NON_AVRO_SCHEMA, content.getSchemaID());

    // broken data
    input = converter.toConnectData(topic, ("123adsada").getBytes(StandardCharsets.UTF_8));
    record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), 0);
    content = assertInstanceOf(SnowflakeRecordContent.class, record.value());

    assertEquals(SnowflakeRecordContent.NON_AVRO_SCHEMA, content.getSchemaID());

    // avro without schema registry
    converter = new SnowflakeAvroConverterWithoutSchemaRegistry();
    URL resource = ConverterTest.class.getResource(TEST_VALUE_FILE_NAME);
    byte[] testFile = Files.readAllBytes(Paths.get(resource.getFile()));
    input = converter.toConnectData(topic, testFile);
    record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), 0);
    content = assertInstanceOf(SnowflakeRecordContent.class, record.value());

    assertEquals(SnowflakeRecordContent.NON_AVRO_SCHEMA, content.getSchemaID());

    // include schema id
    MockSchemaRegistryClient client = new MockSchemaRegistryClient();
    converter = new SnowflakeAvroConverter();
    ((SnowflakeAvroConverter) converter).setSchemaRegistry(client);
    input = converter.toConnectData(topic, client.getData());
    record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), 0);
    content = assertInstanceOf(SnowflakeRecordContent.class, record.value());
    assertEquals(1, content.getSchemaID());
  }

  protected SchemaAndValue getJsonInputData() {
    return new SnowflakeJsonConverter()
        .toConnectData(topic, ("{\"name\":\"test\"}").getBytes(StandardCharsets.UTF_8));
  }
}
