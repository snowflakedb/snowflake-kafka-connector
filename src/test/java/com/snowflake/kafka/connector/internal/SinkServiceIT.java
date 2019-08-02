package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
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
  public void testIngestion() throws IOException, InterruptedException,
    SQLException
  {
    conn.createTable(table);
    conn.createStage(stage);
    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(1)
        .addTask(table, topic, partition)
        .build();

    JsonNode value = MAPPER.readTree("{\"name\":\"test\"}");
    JsonNode[] values = {value};
    long offset = 0;

    SinkRecord record1 = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", new SnowflakeJsonSchema(), values, offset);

    service.insert(record1);
    List<String> files = conn.listStage(stage,
      FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table,
        partition));

    assert files.size() == 1;
    String fileName = files.get(0);

    assert FileNameUtils.fileNameToTimeIngested(fileName) < System.currentTimeMillis();
    assert FileNameUtils.fileNameToPartition(fileName) == partition;
    assert FileNameUtils.fileNameToStartOffset(fileName) == offset;
    assert FileNameUtils.fileNameToEndOffset(fileName) == offset;

    //wait for ingest
    Thread.sleep(90 * 1000);
    assert TestUtils.tableSize(table) == 1;

    //change cleaner
    Thread.sleep(60 * 1000);
    assert conn.listStage(stage,
      FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table,
        partition)).isEmpty();

    assert service.getOffset(new TopicPartition(topic, partition)) == offset + 1;

    service.close();
    Thread.sleep(60 * 1000);
    assert !conn.pipeExist(pipe);
  }

  @Test
  public void testRecordNumber() throws InterruptedException,
    SQLException, ExecutionException
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

    Future<Integer> result = insert(service, partition, numOfRecord);
    Future<Integer> result1 = insert(service, partition1, numOfRecord1);

    assert result.get() == numOfRecord / numLimit;
    assert result1.get() == numOfRecord1 / numLimit;

    Thread.sleep(90 * 1000);
    assert TestUtils.tableSize(table) == numOfRecord + numOfRecord1;

  }

  private Future<Integer> insert(SnowflakeSinkService sink, int partition,
                                 int numOfRecord)
  {
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    return executorService.submit(
      () ->
      {
        for (int i = 0; i < numOfRecord; i++)
        {
          JsonNode value = MAPPER.readTree("{\"name\":\"test\"}");
          JsonNode[] values = {value};
          sink.insert(
            new SinkRecord(topic, partition, Schema.STRING_SCHEMA
              , "test", new SnowflakeJsonSchema(), values, i));
        }

        return conn.listStage(stage,
          FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table,
            partition)).size();
      }
    );
  }

  @Test
  public void testFileSize() throws ExecutionException, InterruptedException
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

    Future<Integer> result = insert(service, partition, numOfRecord);

    assert result.get() == numOfRecord / (size / 152 + 1);
  }

  @Test
  public void testFlushTime() throws InterruptedException, ExecutionException
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

    assert insert(service, partition, numOfRecord).get() == 0;

    Thread.sleep(flushTime * 1000 + 5);

    assert conn.listStage(stage,
      FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table,
        partition)).size() == 1;

  }

  @Test
  public void testRecover() throws InterruptedException
  {
    String data = "{\"content\":{\"name\":\"test\"},\"meta\":{\"offset\":0," +
      "\"topic\":\"test\",\"partition\":0}}";

    String fileName1 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 0, 0);

    String fileName2 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 1, 1);

    conn.createStage(stage);
    conn.createTable(table);
    conn.createPipe(table, stage, pipe);

    SnowflakeIngestionService ingestionService =
      conn.buildIngestService(stage, pipe);

    conn.put(stage, fileName1, data);
    conn.put(stage, fileName2, data);

    ingestionService.ingestFile(fileName1);

    Thread.sleep(90 * 1000);

    assert conn.listStage(stage,
      FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, 0)).size() == 2;

    SnowflakeSinkServiceFactory.builder(conn)
      .addTask(table, topic, partition)
      .build();

    Thread.sleep(10 * 1000); //wait a few second, s3 consistency issue

    assert conn.listStage(stage,
      FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, 0)).size() == 1;

    Thread.sleep(90 * 1000);

    assert conn.listStage(stage,
      FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, 0)).size() == 0;
  }

  @Test
  public void testBrokenRecord()
  {
    conn.createTable(table);
    conn.createStage(stage);
    String topic = "test";
    int partition = 1;
    long offset = 123;
    byte[] data = "as12321".getBytes();
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result = converter.toConnectData(topic, data);
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
    assert FileNameUtils.fileNameToPartition(name) == partition;
    assert FileNameUtils.fileNameToStartOffset(name) == offset;
    assert FileNameUtils.fileNameToEndOffset(name) == offset;
  }
}
