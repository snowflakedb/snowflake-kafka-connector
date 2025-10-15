package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_V2_ENABLED;
import static com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2.partitionChannelKey;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.codahale.metrics.Gauge;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SnowflakeSinkServiceV2IT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
  private Map<String, String> config;
  private String pipe;

  @BeforeEach
  public void setup() {
    config = TestUtils.getConfForStreaming();
    conn.createTable(table);
    pipe = PipeNameProvider.pipeName(config, table);
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(table);
    TestUtils.dropPipe(pipe);
  }

  @Test
  public void testChannelCloseIngestion() throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    // opens a channel for partition 0, table and topic
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

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
    service.close(Collections.singletonList(topicPartition));

    // Lets insert a record when partition was closed.
    // It should auto create the channel
    service.insert(record1);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);

    service.closeAll();
  }

  @Test
  public void testRebalanceOpenCloseIngestion() throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    // opens a channel for partition 0, table and topic
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

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
    service.close(Collections.singletonList(topicPartition));

    // it should skip this record1 since it will fetch offset token 0 from Snowflake
    service.insert(record1);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);

    service.closeAll();
  }

  @Test
  public void testStreamingIngestion() throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    // opens a channel for partition 0, table and topic
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

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

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);

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
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 3, 20, 5);

    service.closeAll();
  }

  @Test
  public void testStreamingIngest_multipleChannelPartitions_withMetrics()
      throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
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

    service.startPartition(table, topicPartition);
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
        () -> service.getOffset(topicPartition) == recordsInPartition1, 20, 5);
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
    verifyPartitionMetrics(
        metricRegistry,
        partitionChannelKey(topic, partition),
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        recordsInPartition1 - 1);
    verifyPartitionMetrics(
        metricRegistry,
        partitionChannelKey(topic, partition2),
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        recordsInPartition2 - 1);

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
      long processedOffset) {
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
  public void testStreamingIngest_multipleChannelPartitionsWithTopic2Table()
      throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    final int partitionCount = 3;
    final int recordsInEachPartition = 2;
    final int topicCount = 3;

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
  public void testStreamingIngest_startPartitionsWithMultipleChannelPartitions()
      throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    final int partitionCount = 5;
    final int recordsInEachPartition = 2;

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

  @Test
  public void testNativeJsonInputIngestion() throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    // json without schema
    JsonConverter converter = new JsonConverter();
    HashMap<String, String> converterConfig = new HashMap<>();
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
    service.startPartition(table, topicPartition);

    service.insert(noSchemaRecordValue);
    service.insert(schemaRecordValue);

    service.insert(noSchemaRecordKey);
    service.insert(schemaRecordKey);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == endOffset + 1, 20, 5);

    service.closeAll();
  }

  @Test
  public void testNativeAvroInputIngestion() throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
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

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

    service.insert(avroRecordValue);
    service.insert(avroRecordKey);
    service.insert(avroRecordKeyValue);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == endOffset + 1, 20, 5);

    service.closeAll();
  }

  @Test
  public void testBrokenIngestion() throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
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
    service.startPartition(table, topicPartition);

    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(brokenKeyValue);

    TestUtils.assertWithRetry(
        () -> service.getOffset(topicPartition) == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, 20, 5);

    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedData =
        errorReporter.getReportedRecords();

    assert reportedData.size() == 3;
    assert TestUtils.tableSize(table) == 0
        : "expected: " + 0 + " actual: " + TestUtils.tableSize(table);
  }

  @Test
  public void testBrokenRecordIngestionFollowedUpByValidRecord()
      throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    // Mismatched schema and value
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");
    SchemaAndValue correctInputValue = new SchemaAndValue(Schema.STRING_SCHEMA, "correct");

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
    service.startPartition(table, topicPartition);

    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(correctValue);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 3, 20, 5);

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
  @Test
  public void testBrokenRecordIngestionAfterValidRecord() throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    // Mismatched schema and value
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");
    SchemaAndValue correctInputValue = new SchemaAndValue(Schema.STRING_SCHEMA, "correct");

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
    service.startPartition(table, topicPartition);

    service.insert(correctValue);
    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(anotherCorrectValue);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 4, 20, 5);

    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedData =
        errorReporter.getReportedRecords();

    assert reportedData.size() == 2;

    service.closeAll();
  }

  /* Service start -> Insert -> Close. service start -> fetch the offsetToken, compare and ingest check data */

  @Test
  public void testStreamingIngestionWithExactlyOnceSemanticsNoOverlappingOffsets()
      throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));

    long offset = 0;
    // Create sink record
    SinkRecord record1 =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), offset);

    service.insert(record1);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);
    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);

    service.closeAll();

    // initialize a new sink service
    SnowflakeSinkService service2 =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service2.startPartition(table, topicPartition);
    offset = 1;
    // Create sink record
    SinkRecord record2 =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), offset);

    service2.insert(record2);

    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 2, 30, 20);

    assert service2.getOffset(topicPartition) == offset + 1;

    service2.closeAll();
  }

  /* Service start -> Insert -> Close. service start -> fetch the offsetToken, compare and ingest check data */
  @Test
  public void testStreamingIngestionWithExactlyOnceSemanticsOverlappingOffsets()
      throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

    final long noOfRecords = 10;
    // send regular data
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, partition);

    service.insert(records);

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == noOfRecords, 20, 5);

    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 10, 30, 20);

    service.closeAll();

    // initialize a new sink service
    SnowflakeSinkService service2 =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service2.startPartition(table, topicPartition);

    final long startOffsetAlreadyInserted = 5;
    records =
        TestUtils.createJsonStringSinkRecords(
            startOffsetAlreadyInserted, noOfRecords, topic, partition);

    service2.insert(records);

    final long totalRecordsExpected = noOfRecords + (noOfRecords - startOffsetAlreadyInserted);

    // wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == totalRecordsExpected, 30, 20);

    assert service2.getOffset(topicPartition) == totalRecordsExpected;

    service2.closeAll();
  }

  // note this test relies on testrole_kafka and testrole_kafka_1 roles being granted to test_kafka
  // user
  @Test
  public void testStreamingIngest_multipleChannel_distinctClients()
      throws Exception {
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "false");
    // create cat and dog configs and partitions
    // one client is enabled but two clients should be created because different roles in config
    String catTopic = "catTopic_" + TestUtils.randomTableName();
    Map<String, String> catConfig = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(catConfig);
    catConfig.put(Utils.SF_OAUTH_CLIENT_ID, "1");
    catConfig.put(Utils.NAME, catTopic);

    String dogTopic = "dogTopic_" + TestUtils.randomTableName();
    Map<String, String> dogConfig = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(dogConfig);
    dogConfig.put(Utils.SF_OAUTH_CLIENT_ID, "2");
    dogConfig.put(Utils.NAME, dogTopic);

    String fishTopic = "fishTopic_" + TestUtils.randomTableName();
    Map<String, String> fishConfig = TestUtils.getConfForStreaming();
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
}
