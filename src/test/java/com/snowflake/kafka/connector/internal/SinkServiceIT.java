package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SinkServiceIT
{
  private SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private String table = TestUtils.randomTableName();
  private String stage = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME, table);
  private int partition = 0;
  private int partition1 = 1;
  private String pipe = Utils.pipeName(TestUtils.TEST_CONNECTOR_NAME, table,
    partition);
  private String pipe1 = Utils.pipeName(TestUtils.TEST_CONNECTOR_NAME, table,
    partition1);
  private String topic = "test";
  private static ObjectMapper MAPPER = new ObjectMapper();

  @After
  public void afterEach()
  {
    conn.dropStage(stage);
    conn.dropPipe(pipe);
    conn.dropPipe(pipe1);
    TestUtils.dropTable(table);
  }


  @Test
  public void testSinkServiceBuilder()
  {
    //default value
    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory.builder(conn).build();

    assert service.getFileSize() == SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
    assert service.getFlushTime() == SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT;
    assert service.getRecordNumber() == SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;

    //set some value
    service = SnowflakeSinkServiceFactory.builder(conn)
      .setFileSize(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX - 10)
      .setFlushTime(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN + 10)
      .setRecordNumber(10)
      .build();

    assert service.getRecordNumber() == 10;
    assert service.getFlushTime() == SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN + 10;
    assert service.getFileSize() == SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX - 10;

    //set some invalid value
    service = SnowflakeSinkServiceFactory.builder(conn)
      .setRecordNumber(-100)
      .setFlushTime(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN - 10)
      .setFileSize(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX + 10)
      .build();

    assert service.getFileSize() == SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX;
    assert service.getFlushTime() == SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
    assert service.getRecordNumber() == 0;

    //connection test
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5010, () ->
      SnowflakeSinkServiceFactory.builder(null).build());
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5010, () ->
    {
      SnowflakeConnectionService conn = TestUtils.getConnectionService();
      conn.close();
      SnowflakeSinkServiceFactory.builder(conn).build();
    });
  }

  @Test
  public void testIngestion() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(1)
        .addTask(table, topic, partition)
        .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input = converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record1 = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", input.schema(), input.value(), offset);

    service.insert(record1);
    TestUtils.assertWithRetry(() -> conn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
                                                                                 table, partition)).size() == 1,
                              5, 4);
    service.callAllGetOffset();
    List<String> files = conn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
                                                                        table, partition));
    String fileName = files.get(0);

    assert FileNameUtils.fileNameToTimeIngested(fileName) < System.currentTimeMillis();
    assert FileNameUtils.fileNameToPartition(fileName) == partition;
    assert FileNameUtils.fileNameToStartOffset(fileName) == offset;
    assert FileNameUtils.fileNameToEndOffset(fileName) == offset;

    //wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 10);

    //change cleaner
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition) == 0,30, 10);

    assert service.getOffset(new TopicPartition(topic, partition)) == offset + 1;

    service.closeAll();
    // don't drop pipe in current version
//    assert !conn.pipeExist(pipe);
  }

  @Test
  public void testNativeInputIngestion() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    String jsonWithSchema = "" +
      "{\n" +
      "  \"schema\": {\n" +
      "    \"type\": \"struct\",\n" +
      "    \"fields\": [\n" +
      "      {\n" +
      "        \"type\": \"string\",\n" +
      "        \"optional\": false,\n" +
      "        \"field\": \"regionid\"\n" +
      "      },\n" +
      "      {\n" +
      "        \"type\": \"string\",\n" +
      "        \"optional\": false,\n" +
      "        \"field\": \"gender\"\n" +
      "      }\n" +
      "    ],\n" +
      "    \"optional\": false,\n" +
      "    \"name\": \"ksql.users\"\n" +
      "  },\n" +
      "  \"payload\": {\n" +
      "    \"regionid\": \"Region_5\",\n" +
      "    \"gender\": \"MALE\"\n" +
      "  }\n" +
      "}";
    String jsonWithoutSchema = "{\"userid\": \"User_1\"}";

    JsonConverter converter = new JsonConverter();
    HashMap<String, String> converterConfig = new HashMap<String, String>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, false);
    SchemaAndValue noSchemaInput = converter.toConnectData(topic, jsonWithoutSchema.getBytes(StandardCharsets.UTF_8));

    converter = new JsonConverter();
    converterConfig = new HashMap<String, String>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaInput = converter.toConnectData(topic, jsonWithSchema.getBytes(StandardCharsets.UTF_8));

    long startOffset = 0;
    long endOffset = 1;
    long recordCount = endOffset + 1;

    SinkRecord noSchemaRecord = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", noSchemaInput.schema(), noSchemaInput.value(), startOffset);
    SinkRecord schemaRecord = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", schemaInput.schema(), schemaInput.value(), startOffset + 1);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(recordCount)
        .addTask(table, topic, partition)
        .build();

    service.insert(noSchemaRecord);
    service.insert(schemaRecord);

    TestUtils.assertWithRetry(() ->
            conn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
                                                           table, partition)).size() == 1,
        5, 4);
    service.callAllGetOffset();
    List<String> files = conn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
                                                                        table, partition));
    String fileName = files.get(0);

    assert FileNameUtils.fileNameToTimeIngested(fileName) < System.currentTimeMillis();
    assert FileNameUtils.fileNameToPartition(fileName) == partition;
    assert FileNameUtils.fileNameToStartOffset(fileName) == startOffset;
    assert FileNameUtils.fileNameToEndOffset(fileName) == endOffset;

    //wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == recordCount, 30, 10);

    //change cleaner
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition) == 0,30, 10);

    assert service.getOffset(new TopicPartition(topic, partition)) == recordCount;

    service.closeAll();
    // don't drop pipe in current version
    // assert !conn.pipeExist(pipe);
  }

  @Test
  public void testRecordNumber() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    int numOfRecord = 234;
    int numOfRecord1 = 123;
    int numLimit = 100;

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(numLimit)
        .setFlushTime(30)
        .addTask(table, topic, partition)
        .addTask(table, topic, partition1)
        .build();

    insert(service, partition, numOfRecord);
    insert(service, partition1, numOfRecord1);

    TestUtils.assertWithRetry(
        () -> getStageSize(stage, table, partition) == numOfRecord / numLimit, 5, 4);
    TestUtils.assertWithRetry(
        () -> getStageSize(stage, table, partition1) == numOfRecord1 / numLimit, 5, 4);

    TestUtils.assertWithRetry(() -> {
        service.insert(new ArrayList<>()); // trigger time based flush
        service.callAllGetOffset();
        return TestUtils.tableSize(table) == numOfRecord + numOfRecord1;
      }, 30, 10);

    service.closeAll();
  }

  private void insert(SnowflakeSinkService sink, int partition,
                                 int numOfRecord)
  {
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    executorService.submit(
      () ->
      {
        for (int i = 0; i < numOfRecord; i++)
        {
          SnowflakeConverter converter = new SnowflakeJsonConverter();
          SchemaAndValue input = converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes());

          sink.insert(
            new SinkRecord(topic, partition, Schema.STRING_SCHEMA
              , "test", input.schema(), input.value(), i));
        }
      }
    );
  }

  @Test
  public void testFileSize() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    int numOfRecord = 111; //152 bytes each
    long size = 10000;

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setFileSize(size)
        .setFlushTime(30)
        .addTask(table, topic, partition)
        .build();

    insert(service, partition, numOfRecord);

    TestUtils.assertWithRetry(
        () -> getStageSize(stage, table, partition) == numOfRecord / (size / 152 + 1), 5, 4);

    service.closeAll();
  }

  @Test
  public void testFlushTime() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    int numOfRecord = 111; //152 bytes each
    long flushTime = 20;

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setFlushTime(flushTime)
        .addTask(table, topic, partition)
        .build();

    insert(service, partition, numOfRecord);

    TestUtils.assertWithRetry(
        () -> getStageSize(stage, table, partition) == 0, 5, 4);

    TestUtils.assertWithRetry(() -> {
        service.insert(new ArrayList<>()); // trigger time based flush
        service.callAllGetOffset();
        return getStageSize(stage, table, partition) == 1;
      }, 15, 4);

    service.closeAll();
  }

  @Test
  public void testRecover() throws Exception
  {
    String data = "{\"content\":{\"name\":\"test\"},\"meta\":{\"offset\":0," +
      "\"topic\":\"test\",\"partition\":0}}";

    // Two hours ago
    long time = System.currentTimeMillis() - 120 * 60 * 1000L;

    String fileName1 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 0, 0, time);

    String fileName2 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 1, 1, time);

    conn.createStage(stage);
    conn.createTable(table);
    conn.createPipe(table, stage, pipe);

    SnowflakeIngestionService ingestionService =
      conn.buildIngestService(stage, pipe);

    conn.put(stage, fileName1, data);
    conn.put(stage, fileName2, data);

    ingestionService.ingestFile(fileName1);

    assert getStageSize(stage, table, 0) == 2;

    SnowflakeSinkService service = SnowflakeSinkServiceFactory.builder(conn)
      .addTask(table, topic, partition)
      .setRecordNumber(1) // immediate flush
      .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result = converter.toConnectData(topic, "12321".getBytes(StandardCharsets.UTF_8));
    SinkRecord record = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", result.schema(), result.value(), 1);
    // lazy init and recovery function
    service.insert(record);
    // wait for async put
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, 0) == 3, 5, 10);
    // call snow pipe
    service.callAllGetOffset();
    // cleaner will remove previous files and ingested new file
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, 0) == 0, 30, 10);

    service.closeAll();
  }

  @Test
  public void testBrokenRecord()
  {
    conn.createTable(table);
    conn.createStage(stage);
    String topic = "test";
    int partition = 1;
    long offset = 123;
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result = converter.toConnectData(topic, "as12321".getBytes(StandardCharsets.UTF_8));
    SinkRecord record = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", result.schema(), result.value(), offset);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .addTask(table, topic, partition)
        .build();

    service.insert(record);

    List<String> files = conn.listStage(table, "", true);
    assert files.size() == 1;
    String name = files.get(0);
    assert TestUtils.getPartitionFromBrokenFileName(name) == partition;
    assert TestUtils.getOffsetFromBrokenFileName(name) == offset;

    service.closeAll();
  }

  int getStageSize(String stage, String table, int partition)
  {
    return conn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition)).size();
  }
}
