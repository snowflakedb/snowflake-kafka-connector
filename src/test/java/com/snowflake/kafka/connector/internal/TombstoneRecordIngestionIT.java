package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.ConnectorConfigTest;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TombstoneRecordIngestionIT {
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
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE,
            ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE_STREAMING,
            ConnectorConfigTest.CommunityConverterSubset.STRING_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE_STREAMING,
            ConnectorConfigTest.CommunityConverterSubset.JSON_CONVERTER.converter
          },
          {
            IngestionMethodConfig.SNOWPIPE_STREAMING,
            ConnectorConfigTest.CommunityConverterSubset.AVRO_CONVERTER.converter
          }
        });
  }

  private IngestionMethodConfig ingestionMethod;
  private Converter converter;

  public TombstoneRecordIngestionIT(IngestionMethodConfig ingestionMethod, Converter converter) {
    this.ingestionMethod = ingestionMethod;
    this.converter = converter;
  }

  private final int partition = 0;
  private final String topic = "test";

  private SnowflakeConnectionService conn;
  private String table;

  // snowpipe
  private String stage;
  private String pipe;

  @Before
  public void setup() {
    this.table = TestUtils.randomTableName();

    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      this.conn = TestUtils.getConnectionService();
      this.stage = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME, table);
      this.pipe = Utils.pipeName(TestUtils.TEST_CONNECTOR_NAME, table, partition);
    } else {
      this.conn = TestUtils.getConnectionServiceForStreaming();
    }
  }

  @After
  public void afterEach() {
    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      conn.dropStage(stage);
      conn.dropPipe(pipe);
    } else {

    }

    TestUtils.dropTable(table);
  }

  @Test
  public void testDefaultTombstoneRecordBehavior() throws Exception {
    Map<String, String> connectorConfig = TestUtils.getConfig();

    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      conn.createTable(table);
      conn.createStage(stage);
    } else {
      connectorConfig = TestUtils.getConfForStreaming();
    }

    // set default behavior
    connectorConfig.put(
        SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
        SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT.toString());

    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, this.ingestionMethod, connectorConfig)
            .setRecordNumber(1)
            .addTask(table, new TopicPartition(topic, partition))
            .build();

    // make tombstone record
    SchemaAndValue input = converter.toConnectData(topic, null);
    long offset = 0;
    SinkRecord record1 =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), offset);

    // test insert
    service.insert(Collections.singletonList(record1));

    // verify inserted
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == 1, 20, 5);

    service.closeAll();
  }

  @Test
  public void testIgnoreTombstoneRecordBehavior() throws Exception {
    if (this.ingestionMethod.equals(IngestionMethodConfig.SNOWPIPE)) {
      conn.createTable(table);
      conn.createStage(stage);
    }

    // set default behavior
    Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put(
        SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
        String.valueOf(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.IGNORE));

    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE, connectorConfig)
            .setRecordNumber(1)
            .addTask(table, new TopicPartition(topic, partition))
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
    SchemaAndValue record2Input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
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

    // verify only normal record was ingested to stage
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, partition)) == record2Offset + 1, 20, 5);

    service.closeAll();
  }

  //
  //  @Test
  //  public void testTombstoneRecords_DEFAULT_behavior_ingestion_SFJsonConverter() throws Exception
  // {
  //    conn.createTable(table);
  //    conn.createStage(stage);
  //
  //    Map<String, String> connectorConfig = new HashMap<>();
  //    connectorConfig.put(
  //        SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
  //        SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT.toString());
  //
  //    SnowflakeSinkService service =
  //        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE,
  // connectorConfig)
  //            .setRecordNumber(1)
  //            .addTask(table, new TopicPartition(topic, partition0))
  //            .build();
  //
  //    SnowflakeConverter converter = new SnowflakeJsonConverter();
  //    SchemaAndValue input = converter.toConnectData(topic, null);
  //    long offset = 0;
  //
  //    SinkRecord record1 =
  //        new SinkRecord(
  //            topic, partition0, Schema.STRING_SCHEMA, "test", input.schema(), input.value(),
  // offset);
  //    service.insert(Collections.singletonList(record1));
  //    TestUtils.assertWithRetry(
  //        () ->
  //            conn.listStage(
  //                    stage,
  //                    FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition0))
  //                .size()
  //                == 1,
  //        5,
  //        4);
  //    service.callAllGetOffset();
  //    List<String> files =
  //        conn.listStage(
  //            stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition0));
  //    String fileName = files.get(0);
  //
  //    assert FileNameUtils.fileNameToTimeIngested(fileName) < System.currentTimeMillis();
  //    assert FileNameUtils.fileNameToPartition(fileName) == partition0;
  //    assert FileNameUtils.fileNameToStartOffset(fileName) == offset;
  //    assert FileNameUtils.fileNameToEndOffset(fileName) == offset;
  //
  //    // wait for ingest
  //    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);
  //
  //    ResultSet resultSet = TestUtils.showTable(table);
  //    LinkedList<String> contentResult = new LinkedList<>();
  //    while (resultSet.next()) {
  //      contentResult.add(resultSet.getString("RECORD_CONTENT"));
  //    }
  //    resultSet.close();
  //
  //    assert contentResult.size() == 1;
  //
  //    ObjectNode emptyNode = MAPPER.createObjectNode();
  //    assert contentResult.get(0).equalsIgnoreCase(emptyNode.toString());
  //
  //    // change cleaner
  //    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition0) == 0, 30, 20);
  //
  //    assert service.getOffset(new TopicPartition(topic, partition0)) == offset + 1;
  //
  //    service.closeAll();
  //  }
  //
  //  @Test
  //  public void testTombstoneRecords_IGNORE_behavior_ingestion_SFJsonConverter() throws Exception
  // {
  //    conn.createTable(table);
  //    conn.createStage(stage);
  //
  //    Map<String, String> connectorConfig = new HashMap<>();
  //    connectorConfig.put(
  //        SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
  //        String.valueOf(SnowflakeSinkConnectorConfig.BehaviorOnNullValues.IGNORE));
  //
  //    SnowflakeSinkService service =
  //        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE,
  // connectorConfig)
  //            .setRecordNumber(1)
  //            .addTask(table, new TopicPartition(topic, partition0))
  //            .build();
  //
  //    SnowflakeConverter converter = new SnowflakeJsonConverter();
  //    SchemaAndValue input = converter.toConnectData(topic, null);
  //    long offset = 0;
  //
  //    SinkRecord record1 =
  //        new SinkRecord(
  //            topic, partition0, Schema.STRING_SCHEMA, "test", input.schema(), input.value(),
  // offset);
  //    service.insert(Collections.singletonList(record1));
  //    Assert.assertTrue(
  //        ((SnowflakeSinkServiceV1) service)
  //            .isPartitionBufferEmpty(SnowflakeSinkServiceV1.getNameIndex(topic, partition0)));
  //    TestUtils.assertWithRetry(
  //        () ->
  //            conn.listStage(
  //                    stage,
  //                    FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition0))
  //                .size()
  //                == 0,
  //        5,
  //        4);
  //
  //    // wait for ingest
  //    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 0, 30, 20);
  //
  //    ResultSet resultSet = TestUtils.showTable(table);
  //    Assert.assertTrue(resultSet.getFetchSize() == 0);
  //    resultSet.close();
  //
  //    service.closeAll();
  //  }
  //
  //  @Test
  //  public void testTombstoneRecords_DEFAULT_behavior_ingestion_CommunityJsonConverter()
  //      throws Exception {
  //    conn.createTable(table);
  //    conn.createStage(stage);
  //
  //    Map<String, String> connectorConfig = new HashMap<>();
  //    connectorConfig.put(
  //        SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
  //        SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT.toString());
  //
  //    SnowflakeSinkService service =
  //        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE,
  // connectorConfig)
  //            .setRecordNumber(1)
  //            .addTask(table, new TopicPartition(topic, partition0))
  //            .build();
  //
  //    JsonConverter converter = new JsonConverter();
  //    HashMap<String, String> converterConfig = new HashMap<String, String>();
  //    converterConfig.put("schemas.enable", "false");
  //    converter.configure(converterConfig, false);
  //    SchemaAndValue input = converter.toConnectData(topic, null);
  //    long offset = 0;
  //
  //    SinkRecord record1 =
  //        new SinkRecord(
  //            topic, partition0, Schema.STRING_SCHEMA, "test", input.schema(), input.value(),
  // offset);
  //    service.insert(Collections.singletonList(record1));
  //    TestUtils.assertWithRetry(
  //        () ->
  //            conn.listStage(
  //                    stage,
  //                    FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition0))
  //                .size()
  //                == 1,
  //        5,
  //        4);
  //    service.callAllGetOffset();
  //    List<String> files =
  //        conn.listStage(
  //            stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition0));
  //    String fileName = files.get(0);
  //
  //    assert FileNameUtils.fileNameToTimeIngested(fileName) < System.currentTimeMillis();
  //    assert FileNameUtils.fileNameToPartition(fileName) == partition0;
  //    assert FileNameUtils.fileNameToStartOffset(fileName) == offset;
  //    assert FileNameUtils.fileNameToEndOffset(fileName) == offset;
  //
  //    // wait for ingest
  //    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);
  //
  //    ResultSet resultSet = TestUtils.showTable(table);
  //    LinkedList<String> contentResult = new LinkedList<>();
  //    while (resultSet.next()) {
  //      contentResult.add(resultSet.getString("RECORD_CONTENT"));
  //    }
  //    resultSet.close();
  //
  //    assert contentResult.size() == 1;
  //
  //    ObjectNode emptyNode = new ObjectMapper().createObjectNode();
  //    assert contentResult.get(0).equalsIgnoreCase(emptyNode.toString());
  //
  //    // change cleaner
  //    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition0) == 0, 30, 20);
  //
  //    assert service.getOffset(new TopicPartition(topic, partition0)) == offset + 1;
  //
  //    service.closeAll();
  //  }
  //
  //  @Test
  //  public void testTombstoneRecords_IGNORE_behavior_ingestion_CommunityJsonConverter()
  //      throws Exception {
  //    conn.createTable(table);
  //    conn.createStage(stage);
  //
  //    Map<String, String> connectorConfig = new HashMap<>();
  //    connectorConfig.put(
  //        SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
  //        SnowflakeSinkConnectorConfig.BehaviorOnNullValues.IGNORE.toString());
  //
  //    SnowflakeSinkService service =
  //        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE,
  // connectorConfig)
  //            .setRecordNumber(1)
  //            .addTask(table, new TopicPartition(topic, partition0))
  //            .build();
  //
  //    JsonConverter converter = new JsonConverter();
  //    HashMap<String, String> converterConfig = new HashMap<String, String>();
  //    converterConfig.put("schemas.enable", "false");
  //    converter.configure(converterConfig, false);
  //    SchemaAndValue input = converter.toConnectData(topic, null);
  //    long offset = 0;
  //
  //    SinkRecord record1 =
  //        new SinkRecord(
  //            topic, partition0, Schema.STRING_SCHEMA, "test", input.schema(), input.value(),
  // offset);
  //    service.insert(Collections.singletonList(record1));
  //    Assert.assertTrue(
  //        ((SnowflakeSinkServiceV1) service)
  //            .isPartitionBufferEmpty(SnowflakeSinkServiceV1.getNameIndex(topic, partition0)));
  //    TestUtils.assertWithRetry(
  //        () ->
  //            conn.listStage(
  //                    stage,
  //                    FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition0))
  //                .size()
  //                == 0,
  //        5,
  //        4);
  //
  //    // wait for ingest
  //    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 0, 30, 20);
  //
  //    ResultSet resultSet = TestUtils.showTable(table);
  //    Assert.assertTrue(resultSet.getFetchSize() == 0);
  //    resultSet.close();
  //
  //    service.closeAll();
  //  }

  int getStageSize(String stage, String table, int partition0) {
    return conn.listStage(
            stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition0))
        .size();
  }
}
