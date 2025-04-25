package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2.partitionChannelKey;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.awaitility.Awaitility.await;

import com.codahale.metrics.Gauge;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SchematizationTestUtils;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class SnowflakeSinkServiceV2IT {

  private SnowflakeConnectionService conn;
  private String table = TestUtils.randomTableName();
  private int partition = 0;
  private int partition2 = 1;

  // Topic name should be same as table name. (Only for testing, not necessarily in real deployment)
  private String topic = table;
  private TopicPartition topicPartition = new TopicPartition(topic, partition);

  private SnowflakeConnectionService getConn(boolean useOAuth) {
    if (useOAuth) {
      return TestUtils.getOAuthConnectionServiceForStreaming();
    } else {
      return TestUtils.getConnectionServiceForStreaming();
    }
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(table);
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testChannelCloseIngestion(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    // opens a channel for partition 0, table and topic
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record1 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test_key" + offset,
            input.schema(),
            input.value(),
            offset);

    // Lets close the service
    // Closing a partition == closing a channel
    service.close(Collections.singletonList(new TopicPartition(topic, partition)));

    // Lets insert a record when partition was closed.
    // It should auto create the channel
    service.insert(record1);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 1, 20, 5);

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testRebalanceOpenCloseIngestion(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    // opens a channel for partition 0, table and topic
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record1 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test_key" + offset,
            input.schema(),
            input.value(),
            offset);

    service.insert(record1);

    // Lets close the service
    // Closing a partition == closing a channel
    service.close(Collections.singletonList(new TopicPartition(topic, partition)));

    // it should skip this record1 since it will fetch offset token 0 from Snowflake
    service.insert(record1);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 1, 20, 5);

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testStreamingIngestion(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    // opens a channel for partition 0, table and topic
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record1 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test_key" + offset,
            input.schema(),
            input.value(),
            offset);

    service.insert(record1);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 1, 20, 5);

    // insert another offset and check what we committed
    offset += 1;
    SinkRecord record2 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test_key" + offset,
            input.schema(),
            input.value(),
            offset);
    offset += 1;
    SinkRecord record3 =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test_key" + offset,
            input.schema(),
            input.value(),
            offset);

    service.insert(Arrays.asList(record2, record3));
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 3, 20, 5);

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testStreamingIngest_multipleChannelPartitions_withMetrics(boolean useOAuth)
      throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    // set up telemetry service spy
    SnowflakeConnectionService connectionService = Mockito.spy(this.conn);
    connectionService.createTable(table);
    SnowflakeTelemetryServiceV2 telemetryService =
        Mockito.spy((SnowflakeTelemetryServiceV2) this.conn.getTelemetryClient());
    Mockito.when(connectionService.getTelemetryClient()).thenReturn(telemetryService);

    // opens a channel for partition 0, table and topic
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(connectionService, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withEnableCustomJMXMetrics(true)
            .build();

    service.startPartition(table, new TopicPartition(topic, partition));
    service.startPartition(table, new TopicPartition(topic, partition2));

    final int recordsInPartition1 = 2;
    final int recordsInPartition2 = 5;
    List<SinkRecord> recordsPartition1 =
        TestUtils.createJsonStringSinkRecords(0, recordsInPartition1, topic, partition);

    List<SinkRecord> recordsPartition2 =
        TestUtils.createJsonStringSinkRecords(0, recordsInPartition2, topic, partition2);

    List<SinkRecord> records = new ArrayList<>(recordsPartition1);
    records.addAll(recordsPartition2);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> {
          // This is how we will trigger flush. (Mimicking poll API)
          service.insert(new ArrayList<>()); // trigger time based flush
          return TestUtils.tableSize(table) == recordsInPartition1 + recordsInPartition2;
        },
        10,
        20);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == recordsInPartition1,
        20,
        5);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition2)) == recordsInPartition2,
        20,
        5);

    // verify all metrics
    Map<String, Gauge> metricRegistry =
        service
            .getMetricRegistry(SnowflakeSinkServiceV2.partitionChannelKey(topic, partition))
            .get()
            .getGauges();
    assert metricRegistry.size()
        == SnowflakeTelemetryChannelStatus.NUM_METRICS * 2; // two partitions

    // partition 1
    this.verifyPartitionMetrics(
        metricRegistry,
        partitionChannelKey(topic, partition),
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        recordsInPartition1 - 1,
        recordsInPartition1,
        1,
        this.conn.getConnectorName());
    this.verifyPartitionMetrics(
        metricRegistry,
        partitionChannelKey(topic, partition2),
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        recordsInPartition2 - 1,
        recordsInPartition2,
        1,
        this.conn.getConnectorName());

    // verify telemetry
    Mockito.verify(telemetryService, Mockito.times(2))
        .reportKafkaPartitionStart(Mockito.any(SnowflakeTelemetryChannelCreation.class));

    service.closeAll();

    // verify metrics closed
    assert !service
        .getMetricRegistry(SnowflakeSinkServiceV2.partitionChannelKey(topic, partition))
        .isPresent();

    Mockito.verify(telemetryService, Mockito.times(2))
        .reportKafkaPartitionUsage(
            Mockito.any(SnowflakeTelemetryChannelStatus.class), Mockito.eq(true));
  }

  private void verifyPartitionMetrics(
      Map<String, Gauge> metricRegistry,
      String partitionChannelKey,
      long offsetPersistedInSnowflake,
      long processedOffset,
      long latestConsumerOffset,
      long currentTpChannelOpenCount,
      String connectorName) {
    // offsets
    assert (long)
            metricRegistry
                .get(
                    MetricsUtil.constructMetricName(
                        partitionChannelKey,
                        MetricsUtil.OFFSET_SUB_DOMAIN,
                        MetricsUtil.OFFSET_PERSISTED_IN_SNOWFLAKE))
                .getValue()
        == offsetPersistedInSnowflake;
    assert (long)
            metricRegistry
                .get(
                    MetricsUtil.constructMetricName(
                        partitionChannelKey,
                        MetricsUtil.OFFSET_SUB_DOMAIN,
                        MetricsUtil.PROCESSED_OFFSET))
                .getValue()
        == processedOffset;
  }

  @Test
  public void testStreamingIngest_multipleChannelPartitionsWithTopic2Table() throws Exception {
    conn = getConn(false);
    final int partitionCount = 3;
    final int recordsInEachPartition = 2;
    final int topicCount = 3;

    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    ArrayList<String> topics = new ArrayList<>();
    for (int topic = 0; topic < topicCount; topic++) {
      topics.add(TestUtils.randomTableName());
    }

    // only insert fist topic to topicTable
    Map<String, String> topic2Table = new HashMap<>();
    topic2Table.put(topics.get(0), table);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withTopicToTableMap(topic2Table)
            .build();

    for (int topic = 0; topic < topicCount; topic++) {
      for (int partition = 0; partition < partitionCount; partition++) {
        service.startPartition(topics.get(topic), new TopicPartition(topics.get(topic), partition));
      }

      List<SinkRecord> records = new ArrayList<>();
      for (int partition = 0; partition < partitionCount; partition++) {
        records.addAll(
            TestUtils.createJsonStringSinkRecords(
                0, recordsInEachPartition, topics.get(topic), partition));
      }

      service.insert(records);
    }

    for (int topic = 0; topic < topicCount; topic++) {
      int finalTopic = topic;
      TestUtils.assertWithRetry(
          () -> {
            service.insert(new ArrayList<>()); // trigger time based flush
            return TestUtils.tableSize(topics.get(finalTopic))
                == recordsInEachPartition * partitionCount;
          },
          10,
          20);

      for (int partition = 0; partition < partitionCount; partition++) {
        int finalPartition = partition;
        TestUtils.assertWithRetry(
            () ->
                service.getOffset(new TopicPartition(topics.get(finalTopic), finalPartition))
                    == recordsInEachPartition,
            20,
            5);
      }
    }

    service.closeAll();
  }

  @Test
  public void testStreamingIngest_startPartitionsWithMultipleChannelPartitions() throws Exception {
    conn = getConn(false);
    final int partitionCount = 5;
    final int recordsInEachPartition = 2;

    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();

    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      topicPartitions.add(new TopicPartition(topic, partition));
    }
    Map<String, String> topic2Table = new HashMap<>();
    topic2Table.put(topic, table);
    service.startPartitions(topicPartitions, topic2Table);

    List<SinkRecord> records = new ArrayList<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      records.addAll(
          TestUtils.createJsonStringSinkRecords(0, recordsInEachPartition, topic, partition));
    }

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> {
          service.insert(new ArrayList<>()); // trigger time based flush
          return TestUtils.tableSize(table) == recordsInEachPartition * partitionCount;
        },
        10,
        20);

    for (int partition = 0; partition < partitionCount; partition++) {
      int finalPartition = partition;
      TestUtils.assertWithRetry(
          () ->
              service.getOffset(new TopicPartition(topic, finalPartition))
                  == recordsInEachPartition,
          20,
          5);
    }

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testStreamingIngestion_timeBased(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    final long noOfRecords = 123;
    List<SinkRecord> sinkRecords =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, partition);

    service.insert(sinkRecords);

    TestUtils.assertWithRetry(
        () -> {
          // This is how we will trigger flush. (Mimicking poll API)
          service.insert(new ArrayList<>()); // trigger time based flush
          return TestUtils.tableSize(table) == noOfRecords;
        },
        10,
        20);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == noOfRecords, 20, 5);

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testNativeJsonInputIngestion(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    // json without schema
    JsonConverter converter = new JsonConverter();
    HashMap<String, String> converterConfig = new HashMap<String, String>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, false);
    SchemaAndValue noSchemaInputValue =
        converter.toConnectData(
            topic, TestUtils.JSON_WITHOUT_SCHEMA.getBytes(StandardCharsets.UTF_8));

    converter = new JsonConverter();
    converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, true);
    SchemaAndValue noSchemaInputKey =
        converter.toConnectData(
            topic, TestUtils.JSON_WITHOUT_SCHEMA.getBytes(StandardCharsets.UTF_8));

    // json with schema
    converter = new JsonConverter();
    converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaInputValue =
        converter.toConnectData(topic, TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8));

    converter = new JsonConverter();
    converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, true);
    SchemaAndValue schemaInputKey =
        converter.toConnectData(topic, TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8));

    long startOffset = 0;
    long endOffset = 3;

    SinkRecord noSchemaRecordValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            noSchemaInputValue.schema(),
            noSchemaInputValue.value(),
            startOffset);
    SinkRecord schemaRecordValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            schemaInputValue.schema(),
            schemaInputValue.value(),
            startOffset + 1);

    SinkRecord noSchemaRecordKey =
        new SinkRecord(
            topic,
            partition,
            noSchemaInputKey.schema(),
            noSchemaInputKey.value(),
            Schema.STRING_SCHEMA,
            "test",
            startOffset + 2);
    SinkRecord schemaRecordKey =
        new SinkRecord(
            topic,
            partition,
            schemaInputKey.schema(),
            schemaInputKey.value(),
            Schema.STRING_SCHEMA,
            "test",
            startOffset + 3);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    service.insert(noSchemaRecordValue);
    service.insert(schemaRecordValue);

    service.insert(noSchemaRecordKey);
    service.insert(schemaRecordKey);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == endOffset + 1, 20, 5);

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testNativeAvroInputIngestion(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    // avro
    SchemaBuilder schemaBuilder =
        SchemaBuilder.struct()
            .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("int8Min", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
            .field("int16Min", Schema.INT16_SCHEMA)
            .field("int32Min", Schema.INT32_SCHEMA)
            .field("int64Min", Schema.INT64_SCHEMA)
            .field("float32Min", Schema.FLOAT32_SCHEMA)
            .field("float64Min", Schema.FLOAT64_SCHEMA)
            .field("int8Max", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
            .field("int16Max", Schema.INT16_SCHEMA)
            .field("int32Max", Schema.INT32_SCHEMA)
            .field("int64Max", Schema.INT64_SCHEMA)
            .field("float32Max", Schema.FLOAT32_SCHEMA)
            .field("float64Max", Schema.FLOAT64_SCHEMA)
            .field("float64HighPrecision", Schema.FLOAT64_SCHEMA)
            .field("float64TenDigits", Schema.FLOAT64_SCHEMA)
            .field("float64BigDigits", Schema.FLOAT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("bytesReadOnly", Schema.BYTES_SCHEMA)
            .field("int16Optional", Schema.OPTIONAL_INT16_SCHEMA)
            .field("int32Optional", Schema.OPTIONAL_INT32_SCHEMA)
            .field("int64Optional", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float32Optional", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("float64Optional", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("booleanOptional", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("stringOptional", Schema.OPTIONAL_STRING_SCHEMA)
            .field("bytesOptional", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .field(
                "int8Optional",
                SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
            .field(
                "mapNonStringKeys",
                SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
            .field(
                "mapArrayMapInt",
                SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                                    .build())
                            .build())
                    .build());
    Struct original =
        new Struct(schemaBuilder.build())
            .put("int8", (byte) 12)
            .put("int16", (short) 12)
            .put("int32", 12)
            .put("int64", 12L)
            .put("float32", 12.2f)
            .put("float64", 12.2)
            .put("int8Min", Byte.MIN_VALUE)
            .put("int16Min", Short.MIN_VALUE)
            .put("int32Min", Integer.MIN_VALUE)
            .put("int64Min", Long.MIN_VALUE)
            .put("float32Min", Float.MIN_VALUE)
            .put("float64Min", Double.MIN_VALUE)
            .put("int8Max", Byte.MAX_VALUE)
            .put("int16Max", Short.MAX_VALUE)
            .put("int32Max", Integer.MAX_VALUE)
            .put("int64Max", Long.MAX_VALUE)
            .put("float32Max", Float.MAX_VALUE)
            .put("float64Max", Double.MAX_VALUE)
            .put("float64HighPrecision", 2312.4200000000001d)
            .put("float64TenDigits", 1.0d / 3.0d)
            .put("float64BigDigits", 2312.42321432655123456d)
            .put("boolean", true)
            .put("string", "foo")
            .put("bytes", ByteBuffer.wrap("foo".getBytes()))
            .put("bytesReadOnly", ByteBuffer.wrap("foo".getBytes()).asReadOnlyBuffer())
            .put("array", Arrays.asList("a", "b", "c"))
            .put("map", Collections.singletonMap("field", 1))
            .put("mapNonStringKeys", Collections.singletonMap(1, 1))
            .put(
                "mapArrayMapInt",
                Collections.singletonMap(
                    "field",
                    Arrays.asList(
                        Collections.singletonMap("field", 1),
                        Collections.singletonMap("field", 1))));

    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
    byte[] converted = avroConverter.fromConnectData(topic, original.schema(), original);
    SchemaAndValue avroInputValue = avroConverter.toConnectData(topic, converted);

    avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"), true);
    converted = avroConverter.fromConnectData(topic, original.schema(), original);
    SchemaAndValue avroInputKey = avroConverter.toConnectData(topic, converted);

    long startOffset = 0;
    long endOffset = 2;

    SinkRecord avroRecordValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            avroInputValue.schema(),
            avroInputValue.value(),
            startOffset);

    SinkRecord avroRecordKey =
        new SinkRecord(
            topic,
            partition,
            avroInputKey.schema(),
            avroInputKey.value(),
            Schema.STRING_SCHEMA,
            "test",
            startOffset + 1);

    SinkRecord avroRecordKeyValue =
        new SinkRecord(
            topic,
            partition,
            avroInputKey.schema(),
            avroInputKey.value(),
            avroInputKey.schema(),
            avroInputKey.value(),
            startOffset + 2);

    conn.createTable(table);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    service.insert(avroRecordValue);
    service.insert(avroRecordKey);
    service.insert(avroRecordKeyValue);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == endOffset + 1, 20, 5);

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testBrokenIngestion(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    // Mismatched schema and value
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");

    long startOffset = 0;

    SinkRecord brokenValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            brokenInputValue.schema(),
            brokenInputValue.value(),
            startOffset);

    SinkRecord brokenKey =
        new SinkRecord(
            topic,
            partition,
            brokenInputValue.schema(),
            brokenInputValue.value(),
            Schema.STRING_SCHEMA,
            "test",
            startOffset + 1);

    SinkRecord brokenKeyValue =
        new SinkRecord(
            topic,
            partition,
            brokenInputValue.schema(),
            brokenInputValue.value(),
            brokenInputValue.schema(),
            brokenInputValue.value(),
            startOffset + 2);
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(brokenKeyValue);

    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, partition))
                == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        20,
        5);

    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedData =
        errorReporter.getReportedRecords();

    assert reportedData.size() == 3;
    assert TestUtils.tableSize(table) == 0
        : "expected: " + 0 + " actual: " + TestUtils.tableSize(table);
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testBrokenRecordIngestionFollowedUpByValidRecord(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    // Mismatched schema and value
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");
    SchemaAndValue correctInputValue = new SchemaAndValue(Schema.STRING_SCHEMA, "correct");

    long recordCount = 1;

    SinkRecord brokenValue =
        new SinkRecord(
            topic, partition, null, null, brokenInputValue.schema(), brokenInputValue.value(), 0);

    SinkRecord brokenKey =
        new SinkRecord(
            topic, partition, brokenInputValue.schema(), brokenInputValue.value(), null, null, 1);

    SinkRecord correctValue =
        new SinkRecord(
            topic, partition, null, null, correctInputValue.schema(), correctInputValue.value(), 2);

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(correctValue);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 3, 20, 5);

    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedData =
        errorReporter.getReportedRecords();

    assert reportedData.size() == 2;
    assert TestUtils.tableSize(table) == 1
        : "expected: " + 1 + " actual: " + TestUtils.tableSize(table);

    service.closeAll();
  }

  /**
   * A bit different from above test where we first insert a valid json record, followed by two
   * broken records (Non valid JSON) followed by another good record with max buffer record size
   * being 2
   */
  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testBrokenRecordIngestionAfterValidRecord(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    // Mismatched schema and value
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");
    SchemaAndValue correctInputValue = new SchemaAndValue(Schema.STRING_SCHEMA, "correct");

    long recordCount = 2;

    SinkRecord correctValue =
        new SinkRecord(
            topic, partition, null, null, correctInputValue.schema(), correctInputValue.value(), 0);

    SinkRecord brokenValue =
        new SinkRecord(
            topic, partition, null, null, brokenInputValue.schema(), brokenInputValue.value(), 1);

    SinkRecord brokenKey =
        new SinkRecord(
            topic, partition, brokenInputValue.schema(), brokenInputValue.value(), null, null, 2);

    SinkRecord anotherCorrectValue =
        new SinkRecord(
            topic, partition, null, null, correctInputValue.schema(), correctInputValue.value(), 3);

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withErrorReporter(errorReporter)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    service.insert(correctValue);
    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(anotherCorrectValue);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 4, 20, 5);

    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedData =
        errorReporter.getReportedRecords();

    assert reportedData.size() == 2;

    service.closeAll();
  }

  /* Service start -> Insert -> Close. service start -> fetch the offsetToken, compare and ingest check data */

  @Test
  public void testStreamingIngestionWithExactlyOnceSemanticsNoOverlappingOffsets()
      throws Exception {
    conn = getConn(false);

    conn.createTable(table);
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));

    long offset = 0;
    // Create sink record
    SinkRecord record1 =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), offset);

    service.insert(record1);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 1, 20, 5);
    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);

    service.closeAll();

    // initialize a new sink service
    SnowflakeSinkService service2 =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(table, new TopicPartition(topic, partition))
            .build();
    offset = 1;
    // Create sink record
    SinkRecord record2 =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), offset);

    service2.insert(record2);

    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 2, 30, 20);

    assert service2.getOffset(new TopicPartition(topic, partition)) == offset + 1;

    service2.closeAll();
  }

  /* Service start -> Insert -> Close. service start -> fetch the offsetToken, compare and ingest check data */

  @Test
  public void testStreamingIngestionWithExactlyOnceSemanticsOverlappingOffsets() throws Exception {
    conn = getConn(false);
    conn.createTable(table);
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));

    long offset = 0;
    final long noOfRecords = 10;
    // send regular data
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, partition);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == noOfRecords, 20, 5);

    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 10, 30, 20);

    service.closeAll();

    // initialize a new sink service
    SnowflakeSinkService service2 =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service2.startPartition(table, new TopicPartition(topic, partition));

    final long startOffsetAlreadyInserted = 5;
    records =
        TestUtils.createJsonStringSinkRecords(
            startOffsetAlreadyInserted, noOfRecords, topic, partition);

    service2.insert(records);

    final long totalRecordsExpected = noOfRecords + (noOfRecords - startOffsetAlreadyInserted);

    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == totalRecordsExpected, 30, 20);

    assert service2.getOffset(new TopicPartition(topic, partition)) == totalRecordsExpected;

    service2.closeAll();
  }

  @Test
  public void testSchematizationWithTableCreationAndJsonInput() throws Exception {
    conn = getConn(false);
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    config.put("schemas.enable", "false");
    // get rid of these at the end
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    SchemaBuilder schemaBuilder =
        SchemaBuilder.struct()
            .field("id_int8", Schema.INT8_SCHEMA)
            .field("id_int8_optional", Schema.OPTIONAL_INT8_SCHEMA)
            .field("id_int16", Schema.INT16_SCHEMA)
            .field("\"id_int32_double_quotes\"", Schema.INT32_SCHEMA)
            .field("id_int64", Schema.INT64_SCHEMA)
            .field("first_name", Schema.STRING_SCHEMA)
            .field("rating_float32", Schema.FLOAT32_SCHEMA)
            .field("rating_float64", Schema.FLOAT64_SCHEMA)
            .field("approval", Schema.BOOLEAN_SCHEMA)
            .field("info_array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field(
                "info_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build());

    Struct original =
        new Struct(schemaBuilder.build())
            .put("id_int8", (byte) 0)
            .put("id_int16", (short) 42)
            .put("\"id_int32_double_quotes\"", 42)
            .put("id_int64", 42L)
            .put("first_name", "zekai")
            .put("rating_float32", 0.99f)
            .put("rating_float64", 0.99d)
            .put("approval", true)
            .put("info_array", Arrays.asList("a", "b"))
            .put("info_map", Collections.singletonMap("field", 3));

    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(config, false);
    byte[] converted = jsonConverter.fromConnectData(topic, original.schema(), original);
    conn.createTableWithOnlyMetadataColumn(table);

    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    long startOffset = 0;

    SinkRecord jsonRecordValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            jsonInputValue.schema(),
            jsonInputValue.value(),
            startOffset);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    // The first insert should fail and schema evolution will kick in to update the schema
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, partition))
                == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        20,
        5);
    TestUtils.checkTableSchema(table, SchematizationTestUtils.SF_JSON_SCHEMA_FOR_TABLE_CREATION);

    // Retry the insert should succeed now with the updated schema
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == startOffset + 1, 20, 5);

    TestUtils.checkTableContentOneRow(
        table, SchematizationTestUtils.CONTENT_FOR_JSON_TABLE_CREATION);

    service.closeAll();
  }

  @Test
  public void testSchematizationSchemaEvolutionWithNonNullableColumn() throws Exception {
    conn = getConn(false);
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    config.put("schemas.enable", "false");
    // get rid of these at the end
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    Schema schema =
        SchemaBuilder.struct()
            .field("id_int8", Schema.INT8_SCHEMA)
            .field("id_int8_non_nullable_null_value", Schema.OPTIONAL_INT8_SCHEMA)
            .build();
    Struct original =
        new Struct(schema).put("id_int8", (byte) 0).put("id_int8_non_nullable_null_value", null);

    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(config, false);
    byte[] converted = jsonConverter.fromConnectData(topic, original.schema(), original);
    conn.createTableWithOnlyMetadataColumn(table);
    createNonNullableColumn(table, "id_int8_non_nullable_missing_value");
    createNonNullableColumn(table, "id_int8_non_nullable_null_value");

    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    long startOffset = 0;

    SinkRecord jsonRecordValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            jsonInputValue.schema(),
            jsonInputValue.value(),
            startOffset);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    // The first insert should fail and schema evolution will kick in to add the column
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, partition))
                == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        20,
        5);

    // The second insert should fail again and schema evolution will kick in to update the
    // first not-nullable column nullability
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, partition))
                == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        20,
        5);

    // The third insert should fail again and schema evolution will kick in to update the
    // second not-nullable column nullability
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, partition))
                == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        20,
        5);

    // Retry the insert should succeed now with the updated schema
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == startOffset + 1, 20, 5);

    service.closeAll();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testStreamingIngestionValidClientLag(boolean useOAuth) throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = getConfig(useOAuth);
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "30");
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    final long noOfRecords = 50;
    List<SinkRecord> sinkRecords =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, partition);

    service.insert(sinkRecords);

    try {
      // Wait 20 seconds here and no flush should happen since the max client lag is 30 seconds
      TestUtils.assertWithRetry(
          () -> service.getOffset(new TopicPartition(topic, partition)) == noOfRecords, 5, 4);
      Assertions.fail("The rows should not be flushed");
    } catch (Exception e) {
      // do nothing
    }

    // Wait for enough time, the rows should be flushed
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == noOfRecords, 30, 30);

    service.closeAll();
  }

  @Test
  public void testStreamingIngestionInvalidClientLag() {
    conn = getConn(false);
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    Map<String, String> overriddenConfig = new HashMap<>(config);
    overriddenConfig.put(
        SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "TWOO_HUNDRED");

    conn.createTable(table);

    try {
      // This will fail in creation of client
      SnowflakeSinkService service =
          StreamingSinkServiceBuilder.builder(conn, config)
              .withSinkTaskContext(
                  new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
              .build();
      service.startPartition(table, new TopicPartition(topic, partition));
    } catch (IllegalArgumentException ex) {
      Assertions.assertEquals(NumberFormatException.class, ex.getCause().getClass());
    }
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testStreamingIngestionValidClientPropertiesOverride(boolean useOAuth)
      throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = new HashMap<>(getConfig(useOAuth));
    config.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES:10000000,ENABLE_SNOWPIPE_STREAMING_JMX_METRICS:false");
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    final long noOfRecords = 10;
    List<SinkRecord> sinkRecords =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, partition);

    service.insert(sinkRecords);

    try {
      // Wait 20 seconds here and no flush should happen since the max client lag is 30 seconds
      TestUtils.assertWithRetry(
          () -> service.getOffset(new TopicPartition(topic, partition)) == noOfRecords, 5, 4);
    } catch (Exception e) {
      // do nothing
    }
    service.closeAll();
  }

  /**
   * Even if override key is invalid, we will still create a client since we dont verify key and
   * values, only format.
   */
  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testStreamingIngestion_invalidClientPropertiesOverride(boolean useOAuth)
      throws Exception {
    conn = getConn(useOAuth);
    Map<String, String> config = new HashMap<>(getConfig(useOAuth));
    config.put(SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "MAX_SOMETHING_SOMETHING:1");
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    conn.createTable(table);

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    final long noOfRecords = 10;
    List<SinkRecord> sinkRecords =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, partition);

    service.insert(sinkRecords);

    try {
      // Wait 20 seconds here and no flush should happen since the max client lag is 30 seconds
      TestUtils.assertWithRetry(
          () -> service.getOffset(new TopicPartition(topic, partition)) == noOfRecords, 5, 4);
    } catch (Exception e) {
      // do nothing
    }
    service.closeAll();
  }

  @Test
  public void snowflakeSinkTask_put_whenJsonRecordCannotBeSchematized_sendRecordToDLQ() {
    // given
    conn = getConn(false);
    Map<String, String> config = new HashMap<>(getConfig(false));
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();
    String notSchematizeableJsonRecord =
        "[{\"name\":\"sf\",\"answer\":42}]"; // cannot schematize array
    byte[] valueContents = (notSchematizeableJsonRecord).getBytes(StandardCharsets.UTF_8);
    SchemaAndValue sv = jsonConverter.toConnectData(topic, valueContents);

    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition).withSchemaAndValue(sv).build();

    // when
    service.insert(record);

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  private void createNonNullableColumn(String tableName, String colName) {
    String createTableQuery = "alter table identifier(?) add " + colName + " int not null";

    try {
      PreparedStatement stmt = conn.getConnection().prepareStatement(createTableQuery);
      stmt.setString(1, tableName);
      stmt.setString(2, colName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2007.getException(e);
    }
  }

  private Map<String, String> getConfig(boolean useOAuth) {
    if (!useOAuth) {
      return TestUtils.getConfForStreaming();
    } else {
      return TestUtils.getConfForStreamingWithOAuth();
    }
  }

  // note this test relies on testrole_kafka and testrole_kafka_1 roles being granted to test_kafka
  // user
  // todo SNOW-1528892: This test does not pass for oAuth turned on - investigate it and fix
  @Test
  public void testStreamingIngest_multipleChannel_distinctClients() throws Exception {
    boolean useOAuth = false;

    conn = getConn(useOAuth);
    // create cat and dog configs and partitions
    // one client is enabled but two clients should be created because different roles in config
    String catTopic = "catTopic_" + TestUtils.randomTableName();
    Map<String, String> catConfig = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(catConfig);
    catConfig.put(Utils.SF_OAUTH_CLIENT_ID, "1");
    catConfig.put(Utils.NAME, catTopic);

    String dogTopic = "dogTopic_" + TestUtils.randomTableName();
    Map<String, String> dogConfig = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(dogConfig);
    dogConfig.put(Utils.SF_OAUTH_CLIENT_ID, "2");
    dogConfig.put(Utils.NAME, dogTopic);

    String fishTopic = "fishTopic_" + TestUtils.randomTableName();
    Map<String, String> fishConfig = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(fishConfig);
    fishConfig.put(Utils.SF_OAUTH_CLIENT_ID, "2");
    fishConfig.put(Utils.NAME, fishTopic);
    fishConfig.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");

    // setup connection and create tables
    TopicPartition catTp = new TopicPartition(catTopic, 0);
    SnowflakeConnectionService catConn =
        SnowflakeConnectionServiceFactory.builder().setProperties(catConfig).build();
    catConn.createTable(catTopic);

    TopicPartition dogTp = new TopicPartition(dogTopic, 1);
    SnowflakeConnectionService dogConn =
        SnowflakeConnectionServiceFactory.builder().setProperties(dogConfig).build();
    dogConn.createTable(dogTopic);

    TopicPartition fishTp = new TopicPartition(fishTopic, 1);
    SnowflakeConnectionService fishConn =
        SnowflakeConnectionServiceFactory.builder().setProperties(fishConfig).build();
    fishConn.createTable(fishTopic);

    // create the sink services
    SnowflakeSinkService catService =
        StreamingSinkServiceBuilder.builder(catConn, catConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(catTp)))
            .build();
    catService.startPartition(catTopic, catTp);

    SnowflakeSinkService dogService =
        StreamingSinkServiceBuilder.builder(dogConn, dogConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(dogTp)))
            .build();
    dogService.startPartition(dogTopic, dogTp);

    SnowflakeSinkService fishService =
        StreamingSinkServiceBuilder.builder(fishConn, fishConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(fishTp)))
            .build();
    fishService.startPartition(fishTopic, fishTp);

    // create records
    final int catRecordCount = 9;
    final int dogRecordCount = 3;
    final int fishRecordCount = 1;

    List<SinkRecord> catRecords =
        TestUtils.createJsonStringSinkRecords(0, catRecordCount, catTp.topic(), catTp.partition());
    List<SinkRecord> dogRecords =
        TestUtils.createJsonStringSinkRecords(0, dogRecordCount, dogTp.topic(), dogTp.partition());
    List<SinkRecord> fishRecords =
        TestUtils.createJsonStringSinkRecords(
            0, fishRecordCount, fishTp.topic(), fishTp.partition());

    // insert records
    catService.insert(catRecords);
    dogService.insert(dogRecords);
    fishService.insert(fishRecords);

    // check data was ingested
    TestUtils.assertWithRetry(() -> catService.getOffset(catTp) == catRecordCount, 20, 20);
    TestUtils.assertWithRetry(() -> dogService.getOffset(dogTp) == dogRecordCount, 20, 20);
    TestUtils.assertWithRetry(() -> fishService.getOffset(fishTp) == fishRecordCount, 20, 20);

    // verify three clients were created
    assert StreamingClientProvider.getStreamingClientProviderInstance()
        .getRegisteredClients()
        .containsKey(new StreamingClientProperties(catConfig));
    assert StreamingClientProvider.getStreamingClientProviderInstance()
        .getRegisteredClients()
        .containsKey(new StreamingClientProperties(dogConfig));
    assert StreamingClientProvider.getStreamingClientProviderInstance()
        .getRegisteredClients()
        .containsKey(new StreamingClientProperties(fishConfig));

    // close services
    catService.closeAll();
    dogService.closeAll();
    fishService.closeAll();

    // verify three clients were closed
    assert !StreamingClientProvider.getStreamingClientProviderInstance()
        .getRegisteredClients()
        .containsKey(new StreamingClientProperties(catConfig));
    assert !StreamingClientProvider.getStreamingClientProviderInstance()
        .getRegisteredClients()
        .containsKey(new StreamingClientProperties(dogConfig));
    assert !StreamingClientProvider.getStreamingClientProviderInstance()
        .getRegisteredClients()
        .containsKey(new StreamingClientProperties(fishConfig));
  }

  @Test
  void testSkippingOffsetsInSchemaEvolution() throws Exception {
    long maxClientLagSeconds = 1L;
    long schemaEvolutionDelayMs = 3 * 1000L; // must be enough for sdk to flush and commit
    long assertionSleepTimeMs = 6 * 1000L;

    conn = getConn(false);
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    config.put("schemas.enable", "false");
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, String.valueOf(maxClientLagSeconds));
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    // setup a table with a single field
    conn.createTableWithOnlyMetadataColumn(table);
    createNonNullableColumn(table, "id_int8");

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withSchemaEvolutionService(
                new DelayedSchemaEvolutionService(conn, schemaEvolutionDelayMs))
            .build();
    service.startPartition(table, new TopicPartition(topic, partition));

    service.insert(
        Arrays.asList(
            recordWithSingleField(0),
            recordWithSingleField(1),
            recordWithTwoFields(2),
            recordWithTwoFields(3)));

    // wait for processing all records and running ingest sdk thread
    Thread.sleep(assertionSleepTimeMs);

    // records 0 and 1 are ingested, 2 triggers schema evolution, 3 is skipped
    // getOffset() result is returned from preCommit() so Kafka will send next record starting from
    // this offset
    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> service.getOffset(new TopicPartition(topic, partition)) == 2);

    // Kafka sends remaining messages
    service.insert(Arrays.asList(recordWithTwoFields(2), recordWithTwoFields(3)));

    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> service.getOffset(new TopicPartition(topic, partition)) == 4);
  }

  private SinkRecord recordWithSingleField(long offset) {
    Schema schema = SchemaBuilder.struct().field("id_int8", Schema.INT8_SCHEMA).build();
    Struct struct = new Struct(schema).put("id_int8", (byte) 0);
    return getSinkRecord(offset, struct);
  }

  private SinkRecord recordWithTwoFields(long offset) {
    Schema schema =
        SchemaBuilder.struct()
            .field("id_int8", Schema.INT8_SCHEMA)
            .field("id_int8_2", Schema.INT8_SCHEMA)
            .build();
    Struct struct = new Struct(schema).put("id_int8", (byte) 0).put("id_int8_2", (byte) 0);
    return getSinkRecord(offset, struct);
  }

  private SinkRecord getSinkRecord(long offset, Struct struct) {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> config = new HashMap<>();
    config.put("schemas.enable", "false");
    jsonConverter.configure(config, false);
    byte[] converted = jsonConverter.fromConnectData(topic, struct.schema(), struct);
    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    return new SinkRecord(
        topic,
        partition,
        Schema.STRING_SCHEMA,
        "test",
        jsonInputValue.schema(),
        jsonInputValue.value(),
        offset);
  }
}
