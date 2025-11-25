package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.ConnectorConfigValidatorTest.COMMUNITY_CONVERTER_SUBSET;
import static com.snowflake.kafka.connector.internal.TestUtils.getConnectionServiceForStreaming;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import com.snowflake.kafka.connector.internal.streaming.StreamingSinkServiceBuilder;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TombstoneRecordIngestionIT {
  private final int partition = 0;
  private final String topic = "test";
  private String table;
  private Converter jsonConverter;
  private Map<String, String> converterConfig;

  @BeforeEach
  void beforeEach() {
    this.table = TestUtils.randomTableName();
    getConnectionServiceForStreaming(false)
        .executeQueryWithParameters(
            format(
                "create or replace table %s (record_metadata variant, gender varchar, regionid"
                    + " varchar)",
                table));

    this.jsonConverter = new JsonConverter();
    this.converterConfig = new HashMap<>();
    this.converterConfig.put("schemas.enable", "false");
    this.jsonConverter.configure(this.converterConfig, false);
  }

  @AfterEach
  void afterEach() {
    TestUtils.dropTable(table);
  }

  @ParameterizedTest(name = "behavior: {0}")
  @EnumSource(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.class)
  void testStreamingTombstoneBehavior(SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior)
      throws Exception {
    // setup
    Map<String, String> connectorConfig = TestUtils.getConnectorConfigurationForStreaming(false);
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Map<String, String> topic2Table = new HashMap<>();
    topic2Table.put(topic, table);
    SnowflakeSinkServiceV2 service =
        StreamingSinkServiceBuilder.builder(getConnectionServiceForStreaming(false), connectorConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withTopicToTableMap(topic2Table)
            .withBehaviorOnNullValues(behavior)
            .build();
    service.startPartitions(Collections.singleton(topicPartition));

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
  @EnumSource(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.class)
  @Disabled(
      "The schema evolution is not supported currently with ssv2, when it is this test should be"
          + " enabled and adapted")
  void testStreamingTombstoneBehaviorWithSchematization(
      SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) throws Exception {
    // setup
    Map<String, String> connectorConfig = TestUtils.getConnectorConfigurationForStreaming(false);
    connectorConfig.put(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, "true");
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Map<String, String> topic2Table = new HashMap<>();
    topic2Table.put(topic, table);
    SnowflakeSinkServiceV2 service =
        StreamingSinkServiceBuilder.builder(getConnectionServiceForStreaming(false), connectorConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withTopicToTableMap(topic2Table)
            .withBehaviorOnNullValues(behavior)
            .build();
    service.startPartitions(Collections.singleton(topicPartition));

    // create one normal record
    SinkRecord normalRecord = TestUtils.createNativeJsonSinkRecords(0, 1, topic, partition).get(0);
    service.insert(normalRecord); // schematization needs first insert for evolution

    // test
    this.testIngestTombstoneRunner(normalRecord, COMMUNITY_CONVERTER_SUBSET, service, behavior);

    // cleanup
    service.closeAll();
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

    // verify inserted (offset updates happen automatically in streaming)
    int expectedOffset =
        behavior == SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT
            ? sinkRecords.size()
            : 1;
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == expectedOffset, 10, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == expectedOffset, 10, 20);

    // assert that one row have values in those columns
    assertThat(
            TestUtils.getTableRows(table).stream()
                .filter(
                    row ->
                        "FEMALE".equals(row.get("GENDER"))
                            && "Region_5".equals(row.get("REGIONID")))
                .count())
        .isEqualTo(1);
  }
}
