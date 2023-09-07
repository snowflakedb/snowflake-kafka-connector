package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.ConnectorConfigTest;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import net.snowflake.ingest.internal.apache.commons.math3.analysis.function.Sin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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

  private final SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior;
  public TombstoneRecordIngestionIT(SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) {
    this.behavior = behavior;
    this.table = TestUtils.randomTableName();
  }

  private final int partition = 0;
  private final String topic = "test";
  private final String table;

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
        SnowflakeSinkServiceFactory.builder(TestUtils.getConnectionServiceForStreaming(), IngestionMethodConfig.SNOWPIPE_STREAMING, connectorConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, topicPartition)
            .setBehaviorOnNullValuesConfig(this.behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    int offset = 0;
    Converter jsonConverter = ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter;
    jsonConverter.configure(converterConfig, false);
    byte[] normalRecordData = "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8);
    SchemaAndValue normalRecordInput = jsonConverter.toConnectData(topic, normalRecordData);
    SinkRecord normalRecord =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "normalrecord",
            normalRecordInput.schema(),
            normalRecordInput.value(),
            offset++);

    // create null inputs
    SchemaAndValue nullRecordInput = jsonConverter.toConnectData(topic, null);
    SinkRecord allNullRecord1 = new SinkRecord(topic, partition, null, null, null, null, offset++);
    SinkRecord allNullRecord2 =
        new SinkRecord(topic, partition, null, null, nullRecordInput.schema(), nullRecordInput.value(), offset++);
    SinkRecord allNullRecord3 =
        new SinkRecord(
            topic, partition, nullRecordInput.schema(), nullRecordInput.value(), nullRecordInput.schema(), nullRecordInput.value(), offset++);


    List<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.addAll(Arrays.asList(normalRecord, allNullRecord1, allNullRecord2, allNullRecord3));

    // create tombstone records from each converter
    int finalOffset = offset;
    sinkRecords.addAll(Arrays.stream(ConnectorConfigTest.CommunityConverterSubset.values())
        .map(converter -> {
          Converter currConverter = converter.converter;
          if (converter == ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER) {
            SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
            currConverter = new AvroConverter(schemaRegistry);
            converterConfig.put("schema.registry.url", "http://fake-url");
          }
          currConverter.configure(converterConfig, false);
          SchemaAndValue input = currConverter.toConnectData(topic, null);
          return new SinkRecord(
              topic,
              partition,
              Schema.STRING_SCHEMA,
              converter.toString(),
              input.schema(),
              input.value(),
              converter.ordinal() + finalOffset); // add one for the other records
        }).collect(Collectors.toList()));

    // add all records and test insert
    service.insert(sinkRecords);
    service.callAllGetOffset();

    // verify inserted
    int expectedOffset = this.behavior == SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT ?
        sinkRecords.size():
        1;
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == expectedOffset, 10, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == expectedOffset, 10, 20);

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
        SnowflakeSinkServiceFactory.builder(TestUtils.getConnectionServiceForStreaming(), IngestionMethodConfig.SNOWPIPE_STREAMING, connectorConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, topicPartition)
            .setBehaviorOnNullValuesConfig(this.behavior)
            .build();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");

    // create one normal record
    int offset = 0;
    Converter jsonConverter = ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter;
    jsonConverter.configure(converterConfig, false);
    byte[] normalRecordData = "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8);
    SchemaAndValue normalRecordInput = jsonConverter.toConnectData(topic, normalRecordData);
    SinkRecord normalRecord =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "normalrecord",
            normalRecordInput.schema(),
            normalRecordInput.value(),
            offset++);

    // create null inputs
    SchemaAndValue nullRecordInput = jsonConverter.toConnectData(topic, null);
    SinkRecord allNullRecord1 = new SinkRecord(topic, partition, null, null, null, null, offset++);
    SinkRecord allNullRecord2 =
        new SinkRecord(topic, partition, null, null, nullRecordInput.schema(), nullRecordInput.value(), offset++);
    SinkRecord allNullRecord3 =
        new SinkRecord(
            topic, partition, nullRecordInput.schema(), nullRecordInput.value(), nullRecordInput.schema(), nullRecordInput.value(), offset++);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.addAll(Arrays.asList(normalRecord, allNullRecord1, allNullRecord2, allNullRecord3));

    // create tombstone records from each converter
    int finalOffset = offset;
    sinkRecords.addAll(Arrays.stream(ConnectorConfigTest.CommunityConverterSubset.values())
        .map(converter -> {
          Converter currConverter = converter.converter;
          if (converter == ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER) {
            SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
            currConverter = new AvroConverter(schemaRegistry);
            converterConfig.put("schema.registry.url", "http://fake-url");
          }
          currConverter.configure(converterConfig, false);
          SchemaAndValue input = currConverter.toConnectData(topic, null);
          return new SinkRecord(
              topic,
              partition,
              Schema.STRING_SCHEMA,
              converter.toString(),
              input.schema(),
              input.value(),
              converter.ordinal() + finalOffset); // add one for the other records
        }).collect(Collectors.toList()));

    // add all records and test insert
    service.insert(sinkRecords);
    service.callAllGetOffset();

    // verify inserted
    int expectedOffset = this.behavior == SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT ?
        sinkRecords.size():
        1;
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == expectedOffset, 10, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == expectedOffset, 10, 20);

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
    int offset = 0;
    Converter jsonConverter = ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter;
    jsonConverter.configure(converterConfig, false);
    byte[] normalRecordData = "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8);
    SchemaAndValue normalRecordInput = jsonConverter.toConnectData(topic, normalRecordData);
    SinkRecord normalRecord =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "normalrecord",
            normalRecordInput.schema(),
            normalRecordInput.value(),
            offset++);

    // create null inputs
    SchemaAndValue nullRecordInput = jsonConverter.toConnectData(topic, null);
    SinkRecord allNullRecord1 = new SinkRecord(topic, partition, null, null, null, null, offset++);
    SinkRecord allNullRecord2 =
        new SinkRecord(topic, partition, null, null, nullRecordInput.schema(), nullRecordInput.value(), offset++);
    SinkRecord allNullRecord3 =
        new SinkRecord(
            topic, partition, nullRecordInput.schema(), nullRecordInput.value(), nullRecordInput.schema(), nullRecordInput.value(), offset++);


    List<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.addAll(Arrays.asList(normalRecord, allNullRecord1, allNullRecord2, allNullRecord3));

    // create tombstone records from each community converter
    int communityConverterOffset = offset;
    sinkRecords.addAll(Arrays.stream(ConnectorConfigTest.CommunityConverterSubset.values())
        .map(converter -> {
          Converter currConverter = converter.converter;
          if (converter == ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER) {
            SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
            currConverter = new AvroConverter(schemaRegistry);
            converterConfig.put("schema.registry.url", "http://fake-url");
          }
          currConverter.configure(converterConfig, false);
          SchemaAndValue input = currConverter.toConnectData(topic, null);
          return new SinkRecord(
              topic,
              partition,
              Schema.STRING_SCHEMA,
              converter.toString(),
              input.schema(),
              input.value(),
              converter.ordinal() + communityConverterOffset); // add one for the other records
        }).collect(Collectors.toList()));

    int customConverterOffset = offset + ConnectorConfigTest.CommunityConverterSubset.values().length;
    sinkRecords.addAll(Arrays.stream(ConnectorConfigTest.CustomSfConverter.values())
        .map(converter -> {
          Converter currConverter = converter.converter;
          if (converter == ConnectorConfigTest.CustomSfConverter.AVRO_CONVERTER) {
            SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
            currConverter = new AvroConverter(schemaRegistry);
            converterConfig.put("schema.registry.url", "http://fake-url");
          }
          currConverter.configure(converterConfig, false);
          SchemaAndValue input = currConverter.toConnectData(topic, null);
          return new SinkRecord(
              topic,
              partition,
              Schema.STRING_SCHEMA,
              converter.toString(),
              input.schema(),
              input.value(),
              converter.ordinal() + customConverterOffset); // add one for the other records
        }).collect(Collectors.toList()));

    // add all records and test insert
    service.insert(sinkRecords);
    service.callAllGetOffset();

    // verify inserted
    int expectedOffset = this.behavior == SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT ?
        sinkRecords.size():
        1;
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == expectedOffset, 10, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == expectedOffset, 10, 20);

    // cleanup
    service.closeAll();
    conn.dropStage(stage);
    conn.dropPipe(pipe);
  }
}
