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
  @Parameterized.Parameters(name = "ingestionMethod: {0}, converter: {1}")
  public static Collection<Object[]> input() {
    return Arrays.asList(
        new Object[][] {
          {
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CustomSfConverter.JSON_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CustomSfConverter.AVRO_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CustomSfConverter.AVRO_CONVERTER_WITHOUT_SCHEMA_REGISTRY.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CommunityConverterSubset.STRING_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE_STREAMING,
            ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE_STREAMING,
            ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE_STREAMING,
            ConnectorConfigTest.CommunityConverterSubset.STRING_CONVERTER.converter
          }
        });
  }

  private final IngestionMethodConfig ingestionMethod;
  private final Converter converter;
  private final boolean isAvroConverter;

  public TombstoneRecordIngestionIT(IngestionMethodConfig ingestionMethod, Converter converter) {
    this.ingestionMethod = ingestionMethod;
    this.isAvroConverter = converter.toString().toLowerCase().contains("avro");
    this.table = TestUtils.randomTableName();

    // setup connection
    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      this.conn = TestUtils.getConnectionService();
      this.stage = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME, table);
      this.pipe = Utils.pipeName(TestUtils.TEST_CONNECTOR_NAME, table, partition);
    } else {
      this.conn = TestUtils.getConnectionServiceForStreaming();
    }

    // setup converter
    Map<String, String> converterConfig = new HashMap<>();
    if (this.isAvroConverter) {
      SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
      this.converter = new AvroConverter(schemaRegistry);
      converterConfig.put("schema.registry.url", "http://fake-url");
    } else {
      this.converter = converter;
    }
    converterConfig.put("schemas.enable", "false");
    this.converter.configure(converterConfig, false);
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
    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      conn.dropStage(stage);
      conn.dropPipe(pipe);
    }

    TestUtils.dropTable(table);
  }

  @Test
  public void testDefaultTombstoneAndNullRecordBehavior() throws Exception {
    Map<String, String> connectorConfig = TestUtils.getConfig();

    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      conn.createTable(table);
      conn.createStage(stage);
    } else {
      connectorConfig = TestUtils.getConfForStreaming();
    }

    TopicPartition topicPartition = new TopicPartition(topic, partition);
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, this.ingestionMethod, connectorConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, topicPartition)
            .build();

    // make tombstone records
    SchemaAndValue input = converter.toConnectData(topic, null);
    SinkRecord tombstoneRecord1 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "tombstoneRecord1",
            input.schema(),
            input.value(),
            0);
    SinkRecord tombstoneRecord2 =
        new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "tombstoneRecord2", null, null, 1);
    SinkRecord allNullRecord1 = new SinkRecord(topic, partition, null, null, null, null, 2);
    SinkRecord allNullRecord2 =
        new SinkRecord(topic, partition, null, null, input.schema(), input.value(), 3);
    SinkRecord allNullRecord3 =
        new SinkRecord(
            topic, partition, input.schema(), input.value(), input.schema(), input.value(), 4);

    // test insert
    service.insert(
        Arrays.asList(
            tombstoneRecord1, tombstoneRecord2, allNullRecord1, allNullRecord2, allNullRecord3));
    service.callAllGetOffset();

    // verify inserted
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 5, 30, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 5, 20, 5);

    service.closeAll();
  }

  @Test
  public void testIgnoreTombstoneRecordBehavior() throws Exception {
    Map<String, String> connectorConfig = TestUtils.getConfig();

    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      conn.createTable(table);
      conn.createStage(stage);
    } else {
      connectorConfig = TestUtils.getConfForStreaming();
    }

    TopicPartition topicPartition = new TopicPartition(topic, partition);
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, this.ingestionMethod, connectorConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, topicPartition)
            .setBehaviorOnNullValuesConfig(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.IGNORE)
            .build();

    // make tombstone record
    SchemaAndValue record1Input = converter.toConnectData(topic, null);
    long record1Offset = 0;
    SinkRecord record1 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            record1Input.schema(),
            record1Input.value(),
            record1Offset);

    // make normal record
    byte[] normalRecordData = "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8);
    if (isAvroConverter) {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct().field("int16", Schema.INT16_SCHEMA);

      Struct original = new Struct(schemaBuilder.build()).put("int16", (short) 12);

      normalRecordData = converter.fromConnectData(topic, original.schema(), original);
    }
    SchemaAndValue record2Input = converter.toConnectData(topic, normalRecordData);
    long record2Offset = 1;
    SinkRecord record2 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            record2Input.schema(),
            record2Input.value(),
            record2Offset);

    // test inserting both records
    service.insert(Collections.singletonList(record1));
    service.insert(Collections.singletonList(record2));
    service.callAllGetOffset();

    // verify only normal record was ingested to table
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == record2Offset + 1, 20, 5);

    service.closeAll();
  }
}
