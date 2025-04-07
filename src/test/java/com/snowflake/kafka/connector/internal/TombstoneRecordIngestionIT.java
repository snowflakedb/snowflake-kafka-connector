package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.ConnectorConfigValidatorTest.COMMUNITY_CONVERTER_SUBSET;
import static com.snowflake.kafka.connector.ConnectorConfigValidatorTest.CUSTOM_SNOWFLAKE_CONVERTERS;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TombstoneRecordIngestionIT {
  private final int partition = 0;
  private final String topic = "test";
  private String table;
  private Converter jsonConverter;
  private Map<String, String> converterConfig;

  @BeforeEach
  public void beforeEach() {
    this.table = TestUtils.randomTableName();

    this.jsonConverter = new JsonConverter();
    this.converterConfig = new HashMap<>();
    this.converterConfig.put("schemas.enable", "false");
    this.jsonConverter.configure(this.converterConfig, false);
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(table);
  }

  private static Stream<Arguments> behaviorAndSingleBufferParameters() {
    return Sets.cartesianProduct(
            ImmutableSet.copyOf(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.values()))
        .stream()
        .map(List::toArray)
        .map(Arguments::of);
  }

  @ParameterizedTest(name = "behavior: {0}")
  @MethodSource("behaviorAndSingleBufferParameters")
  public void testStreamingTombstoneBehavior(
      SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) throws Exception {
    // setup
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(
                TestUtils.getConnectionServiceForStreaming(),
                IngestionMethodConfig.SNOWPIPE_STREAMING,
                connectorConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, topicPartition)
            .setBehaviorOnNullValuesConfig(behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    SinkRecord normalRecord = TestUtils.createNativeJsonSinkRecords(0, 1, topic, partition).get(0);

    // test
    this.testIngestTombstoneRunner(normalRecord, COMMUNITY_CONVERTER_SUBSET, service, behavior);

    // cleanup
    service.closeAll();
  }

  @ParameterizedTest(name = "behavior: {0}")
  @MethodSource("behaviorAndSingleBufferParameters")
  public void testStreamingTombstoneBehaviorWithSchematization(
      SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) throws Exception {
    // setup
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, "true");
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(
                TestUtils.getConnectionServiceForStreaming(),
                IngestionMethodConfig.SNOWPIPE_STREAMING,
                connectorConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, topicPartition)
            .setBehaviorOnNullValuesConfig(behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    SinkRecord normalRecord = TestUtils.createNativeJsonSinkRecords(0, 1, topic, partition).get(0);
    service.insert(normalRecord); // schematization needs first insert for evolution

    // test
    this.testIngestTombstoneRunner(normalRecord, COMMUNITY_CONVERTER_SUBSET, service, behavior);

    // cleanup
    service.closeAll();
  }

  @ParameterizedTest
  @EnumSource(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.class)
  public void testSnowpipeTombstoneBehavior(
      SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) throws Exception {
    // setup
    SnowflakeConnectionService conn = TestUtils.getConnectionService();
    Map<String, String> connectorConfig = TestUtils.getConfig();
    String stage = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME, table);
    String pipe = Utils.pipeName(TestUtils.TEST_CONNECTOR_NAME, table, partition);
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE, connectorConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, topicPartition)
            .setBehaviorOnNullValuesConfig(behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    SinkRecord normalRecord = TestUtils.createNativeJsonSinkRecords(0, 1, topic, partition).get(0);

    // test
    List<Converter> converters = new ArrayList<>(COMMUNITY_CONVERTER_SUBSET);
    converters.addAll(CUSTOM_SNOWFLAKE_CONVERTERS);
    this.testIngestTombstoneRunner(normalRecord, converters, service, behavior);

    // cleanup
    service.closeAll();
    conn.dropStage(stage);
    conn.dropPipe(pipe);
  }

  // all ingestion methods should have the same behavior for tombstone records
  private void testIngestTombstoneRunner(
      SinkRecord normalRecord,
      List<Converter> converters,
      SnowflakeSinkService service,
      SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior)
      throws Exception {
    int offset = 1; // normalRecord should be offset 0
    List<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.add(normalRecord);

    // create tombstone records
    SchemaAndValue nullRecordInput = this.jsonConverter.toConnectData(topic, null);
    SinkRecord allNullRecord1 = new SinkRecord(topic, partition, null, null, null, null, offset++);
    SinkRecord allNullRecord2 =
        new SinkRecord(
            topic,
            partition,
            null,
            null,
            nullRecordInput.schema(),
            nullRecordInput.value(),
            offset++);
    SinkRecord allNullRecord3 =
        new SinkRecord(
            topic,
            partition,
            nullRecordInput.schema(),
            nullRecordInput.value(),
            nullRecordInput.schema(),
            nullRecordInput.value(),
            offset++);

    // add tombstone records
    sinkRecords.addAll(Arrays.asList(allNullRecord1, allNullRecord2, allNullRecord3));

    // create and add tombstone records from each converter
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    for (Converter converter : converters) {
      // handle avro converter
      if (converter.toString().contains("io.confluent.connect.avro.AvroConverter")) {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        converter = new AvroConverter(schemaRegistry);
        converterConfig.put("schema.registry.url", "http://fake-url");
      }

      converter.configure(converterConfig, false);
      SchemaAndValue input = converter.toConnectData(topic, null);
      sinkRecords.add(
          new SinkRecord(
              topic,
              partition,
              Schema.STRING_SCHEMA,
              converter.toString(),
              input.schema(),
              input.value(),
              offset));

      offset++;
    }

    // insert all records
    service.insert(sinkRecords);
    service.callAllGetOffset();

    // verify inserted
    int expectedOffset =
        behavior == SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT
            ? sinkRecords.size()
            : 1;
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == expectedOffset, 10, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == expectedOffset, 10, 20);
  }
}
