package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.ConnectorConfigTest.COMMUNITY_CONVERTER_SUBSET;
import static com.snowflake.kafka.connector.ConnectorConfigTest.CUSTOM_SNOWFLAKE_CONVERTERS;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TombstoneRecordIngestionIT {
  @Parameterized.Parameters(name = "behaviorOnNullValues: {0}")
  public static Collection<Object> input() {
    return Arrays.asList(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.values());
  }

  private final int partition = 0;
  private final String topic = "test";
  private final String table;
  private final Converter jsonConverter;
  private final Map<String, String> converterConfig;

  private final SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior;

  public TombstoneRecordIngestionIT(SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) {
    this.behavior = behavior;
    this.table = TestUtils.randomTableName();

    this.jsonConverter = new JsonConverter();
    this.converterConfig = new HashMap<>();
    this.converterConfig.put("schemas.enable", "false");
    this.jsonConverter.configure(this.converterConfig, false);
  }

  @After
  public void afterEach() {
    TestUtils.dropTable(table);
  }

  @Test
  public void testStreamingTombstoneBehavior() throws Exception {
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
            .setBehaviorOnNullValuesConfig(this.behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    SinkRecord normalRecord = TestUtils.createNativeJsonSinkRecords(0, 1, topic, partition).get(0);

    // test
    this.testIngestTombstoneRunner(normalRecord, COMMUNITY_CONVERTER_SUBSET, service);

    // cleanup
    service.closeAll();
  }

  @Test
  public void testStreamingTombstoneBehaviorWithSchematization() throws Exception {
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
            .setBehaviorOnNullValuesConfig(this.behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    SinkRecord normalRecord = TestUtils.createNativeJsonSinkRecords(0, 1, topic, partition).get(0);
    service.insert(normalRecord); // schematization needs first insert for evolution

    // test
    this.testIngestTombstoneRunner(normalRecord, COMMUNITY_CONVERTER_SUBSET, service);

    // cleanup
    service.closeAll();
  }

  @Test
  public void testSnowpipeTombstoneBehavior() throws Exception {
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
            .setBehaviorOnNullValuesConfig(this.behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    SinkRecord normalRecord = TestUtils.createNativeJsonSinkRecords(0, 1, topic, partition).get(0);

    // test
    List<Converter> converters = new ArrayList<>(COMMUNITY_CONVERTER_SUBSET);
    converters.addAll(CUSTOM_SNOWFLAKE_CONVERTERS);
    this.testIngestTombstoneRunner(normalRecord, converters, service);

    // cleanup
    service.closeAll();
    conn.dropStage(stage);
    conn.dropPipe(pipe);
  }

  // all ingestion methods should have the same behavior for tombstone records
  private void testIngestTombstoneRunner(
      SinkRecord normalRecord, List<Converter> converters, SnowflakeSinkService service)
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
        this.behavior == SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT
            ? sinkRecords.size()
            : 1;
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == expectedOffset, 10, 5);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == expectedOffset, 10, 5);
  }
}
