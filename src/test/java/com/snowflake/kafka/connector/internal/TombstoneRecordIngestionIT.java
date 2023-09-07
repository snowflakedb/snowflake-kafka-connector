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
  // sf converters only supported in snowpipe
//  @Parameterized.Parameters(name = "ingestionMethod: {0}, converter: {1}")
//  public static Collection<Object[]> input() {
//    return Arrays.asList(
//        new Object[][] {
////          {
////            IngestionMethodConfig.SNOWPIPE,
////            ConnectorConfigTest.CustomSfConverter.JSON_CONVERTER.converter
////          },
////          {
////            IngestionMethodConfig.SNOWPIPE,
////            ConnectorConfigTest.CustomSfConverter.AVRO_CONVERTER.converter
////          },
////          {
////            IngestionMethodConfig.SNOWPIPE,
////            ConnectorConfigTest.CustomSfConverter.AVRO_CONVERTER_WITHOUT_SCHEMA_REGISTRY.converter
////          },
////          {
////            IngestionMethodConfig.SNOWPIPE,
////            ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter
////          },
////          {
////            IngestionMethodConfig.SNOWPIPE,
////            ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER.converter
////          },
////          {
////            IngestionMethodConfig.SNOWPIPE,
////            ConnectorConfigTest.CommunityConverterSubset.STRING_CONVERTER.converter
////          },
////          {
////            IngestionMethodConfig.SNOWPIPE_STREAMING,
////            ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter
////          },
////          {
////            IngestionMethodConfig.SNOWPIPE_STREAMING,
////            ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER.converter
////          },
//          {
//            IngestionMethodConfig.SNOWPIPE_STREAMING,
//            ConnectorConfigTest.CommunityConverterSubset.STRING_CONVERTER.converter
//          }
//        });
//  }

//  private final IngestionMethodConfig ingestionMethod;
//  private final Converter converter;
//  private final boolean isAvroConverter;
//
//  public TombstoneRecordIngestionIT(IngestionMethodConfig ingestionMethod, Converter converter) {
//    this.ingestionMethod = ingestionMethod;
//    this.isAvroConverter = converter.toString().toLowerCase().contains("avro");
//    this.table = TestUtils.randomTableName();
//
//    // setup connection
//    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
//      this.conn = TestUtils.getConnectionService();
//      this.stage = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME, table);
//      this.pipe = Utils.pipeName(TestUtils.TEST_CONNECTOR_NAME, table, partition);
//    } else {
//      this.conn = TestUtils.getConnectionServiceForStreaming();
//    }
//
//    // setup converter
//    Map<String, String> converterConfig = new HashMap<>();
//    if (this.isAvroConverter) {
//      SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
//      this.converter = new AvroConverter(schemaRegistry);
//      converterConfig.put("schema.registry.url", "http://fake-url");
//    } else {
//      this.converter = converter;
//    }
//    converterConfig.put("schemas.enable", "false");
//    this.converter.configure(converterConfig, false);
//  }


  @Parameterized.Parameters(name = "ingestionMethod: {0}, converter: {1}")
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

  private SnowflakeConnectionService conn;
  private String table;

  // snowpipe
  private String stage;
  private String pipe;

  @After
  public void afterEach() {
//    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
//      conn.dropStage(stage);
//      conn.dropPipe(pipe);
//    }

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

    // create converter inputs
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    List<SinkRecord> converterTombstoneRecords = Arrays.stream(ConnectorConfigTest.CommunityConverterSubset.values())
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
              converter.ordinal()); // add one for the other records
        }).collect(Collectors.toList());

    // create one normal record
    int offset = ConnectorConfigTest.CommunityConverterSubset.values().length;
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
            offset);

    // create null inputs
    SchemaAndValue nullRecordInput = jsonConverter.toConnectData(topic, null);
    SinkRecord allNullRecord1 = new SinkRecord(topic, partition, null, null, null, null, offset++);
    SinkRecord allNullRecord2 =
        new SinkRecord(topic, partition, null, null, nullRecordInput.schema(), nullRecordInput.value(), offset++);
    SinkRecord allNullRecord3 =
        new SinkRecord(
            topic, partition, nullRecordInput.schema(), nullRecordInput.value(), nullRecordInput.schema(), nullRecordInput.value(), offset++);


    // add all records and test insert
    converterTombstoneRecords.addAll(Arrays.asList(normalRecord, allNullRecord1, allNullRecord2, allNullRecord3));
    service.insert(converterTombstoneRecords);
    service.callAllGetOffset();

    // verify inserted
    int expectedOffset = this.behavior == SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT ?
        converterTombstoneRecords.size() - 1 :
        converterTombstoneRecords.size() - ConnectorConfigTest.CommunityConverterSubset.values().length;
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == expectedOffset, 10, 5);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == expectedOffset, 10, 5);

    service.closeAll();
  }

//
//  @Test
//  public void testDefaultTombstoneAndNullRecordBehavior() throws Exception {
//    Map<String, String> connectorConfig = TestUtils.getConfig();
//
//    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
//      conn.createTable(table);
//      conn.createStage(stage);
//    } else {
//      connectorConfig = TestUtils.getConfForStreaming();
//    }
//
//    TopicPartition topicPartition = new TopicPartition(topic, partition);
//    SnowflakeSinkService service =
//        SnowflakeSinkServiceFactory.builder(conn, this.ingestionMethod, connectorConfig)
//            .setRecordNumber(1)
//            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
//            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
//            .addTask(table, topicPartition)
//            .build();
//
//
//    Map<String, String> converterConfig = new HashMap<>();
//    converterConfig.put("schemas.enable", "false");
//
//    Converter jsonConverter = ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter;
//    jsonConverter.configure(converterConfig, false);
//    Converter stringConverter = ConnectorConfigTest.CommunityConverterSubset.STRING_CONVERTER.converter;
//    stringConverter.configure(converterConfig, false);
//
//    // make tombstone records
//    SchemaAndValue jsonInput = jsonConverter.toConnectData(topic, null);
//    SchemaAndValue stringInput = stringConverter.toConnectData(topic, null);
//    SinkRecord jsonTombstone =
//        new SinkRecord(
//            topic,
//            partition,
//            Schema.STRING_SCHEMA,
//            "jsonTombstone",
//            jsonInput.schema(),
//            jsonInput.value(),
//            0);
//    SinkRecord stringTombstone =
//        new SinkRecord(
//            topic,
//            partition,
//            Schema.STRING_SCHEMA,
//            "stringTombstone",
//            stringInput.schema(),
//            stringInput.value(),
//            1);
//
//    SinkRecord tombstoneRecord1 =
//        new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "tombstoneRecord2", null, null, 2);
//    SinkRecord allNullRecord1 = new SinkRecord(topic, partition, null, null, null, null, 3);
//    SinkRecord allNullRecord2 =
//        new SinkRecord(topic, partition, null, null, jsonInput.schema(), jsonInput.value(), 4);
//    SinkRecord allNullRecord3 =
//        new SinkRecord(
//            topic, partition, jsonInput.schema(), jsonInput.value(), jsonInput.schema(), jsonInput.value(), 5);
//
//    // test insert
//    service.insert(
//        Arrays.asList(
//            jsonTombstone, stringTombstone, tombstoneRecord1, allNullRecord1, allNullRecord2, allNullRecord3));
//    service.callAllGetOffset();
//
//    // verify inserted
//    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 6, 30, 20);
//    TestUtils.assertWithRetry(
//        () -> service.getOffset(new TopicPartition(topic, partition)) == 6, 20, 5);
//
//    service.closeAll();
//  }
//
//  @Test
//  public void testIgnoreTombstoneRecordBehavior() throws Exception {
//    Map<String, String> connectorConfig = TestUtils.getConfig();
//
//    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
//      conn.createTable(table);
//      conn.createStage(stage);
//    } else {
//      connectorConfig = TestUtils.getConfForStreaming();
//    }
//
//    TopicPartition topicPartition = new TopicPartition(topic, partition);
//    SnowflakeSinkService service =
//        SnowflakeSinkServiceFactory.builder(conn, this.ingestionMethod, connectorConfig)
//            .setRecordNumber(1)
//            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
//            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
//            .addTask(table, topicPartition)
//            .setBehaviorOnNullValuesConfig(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.IGNORE)
//            .build();
//
//    // make tombstone record
//    SchemaAndValue record1Input = converter.toConnectData(topic, null);
//    long record1Offset = 0;
//    SinkRecord record1 =
//        new SinkRecord(
//            topic,
//            partition,
//            Schema.STRING_SCHEMA,
//            "test",
//            record1Input.schema(),
//            record1Input.value(),
//            record1Offset);
//
//    // make normal record
//    byte[] normalRecordData = "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8);
//    if (isAvroConverter) {
//      SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int16", Schema.INT16_SCHEMA);
//
//      Struct original = new Struct(schemaBuilder.build()).put("int16", (short) 12);
//
//      normalRecordData = converter.fromConnectData(topic, original.schema(), original);
//    }
//    SchemaAndValue record2Input = converter.toConnectData(topic, normalRecordData);
//    long record2Offset = 1;
//    SinkRecord record2 =
//        new SinkRecord(
//            topic,
//            partition,
//            Schema.STRING_SCHEMA,
//            "test",
//            record2Input.schema(),
//            record2Input.value(),
//            record2Offset);
//
//    // test inserting both records
//    service.insert(Collections.singletonList(record1));
//    service.insert(Collections.singletonList(record2));
//    service.callAllGetOffset();
//
//    // verify only normal record was ingested to table
//    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);
//    TestUtils.assertWithRetry(
//        () -> service.getOffset(new TopicPartition(topic, partition)) == record2Offset + 1, 20, 5);
//
//    service.closeAll();
//  }
}
