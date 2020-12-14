package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

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


  private static final String jsonWithSchema = "" +
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
  private static final String jsonWithoutSchema = "{\"userid\": \"User_1\"}";

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
      .setFileSize(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT * 4)
      .setFlushTime(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN + 10)
      .setRecordNumber(10)
      .build();

    assert service.getRecordNumber() == 10;
    assert service.getFlushTime() == SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN + 10;
    assert service.getFileSize() == SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT * 4;

    //set some invalid value
    service = SnowflakeSinkServiceFactory.builder(conn)
      .setRecordNumber(-100)
      .setFlushTime(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN - 10)
      .setFileSize(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN - 1)
      .build();

    assert service.getFileSize() == SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
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
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 30, 20);

    //change cleaner
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition) == 0,30, 20);

    assert service.getOffset(new TopicPartition(topic, partition)) == offset + 1;

    service.closeAll();
    // don't drop pipe in current version
//    assert !conn.pipeExist(pipe);
  }

  @Test
  public void testNativeJsonInputIngestion() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);

    // json without schema
    JsonConverter converter = new JsonConverter();
    HashMap<String, String> converterConfig = new HashMap<String, String>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, false);
    SchemaAndValue noSchemaInputValue = converter.toConnectData(topic, jsonWithoutSchema.getBytes(StandardCharsets.UTF_8));

    converter = new JsonConverter();
    converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, true);
    SchemaAndValue noSchemaInputKey = converter.toConnectData(topic, jsonWithoutSchema.getBytes(StandardCharsets.UTF_8));

    // json with schema
    converter = new JsonConverter();
    converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaInputValue = converter.toConnectData(topic, jsonWithSchema.getBytes(StandardCharsets.UTF_8));

    converter = new JsonConverter();
    converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, true);
    SchemaAndValue schemaInputKey = converter.toConnectData(topic, jsonWithSchema.getBytes(StandardCharsets.UTF_8));

    long startOffset = 0;
    long endOffset = 3;
    long recordCount = endOffset + 1;

    SinkRecord noSchemaRecordValue = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", noSchemaInputValue.schema(), noSchemaInputValue.value(), startOffset);
    SinkRecord schemaRecordValue = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", schemaInputValue.schema(), schemaInputValue.value(), startOffset + 1);


    SinkRecord noSchemaRecordKey = new SinkRecord(topic, partition, noSchemaInputKey.schema(), noSchemaInputKey.value(),
      Schema.STRING_SCHEMA, "test", startOffset + 2);
    SinkRecord schemaRecordKey = new SinkRecord(topic, partition, schemaInputKey.schema(), schemaInputKey.value(),
      Schema.STRING_SCHEMA, "test",startOffset + 3);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(recordCount)
        .addTask(table, topic, partition)
        .build();

    service.insert(noSchemaRecordValue);
    service.insert(schemaRecordValue);

    service.insert(noSchemaRecordKey);
    service.insert(schemaRecordKey);

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
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == recordCount, 30, 20);

    //change cleaner
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition) == 0,30, 20);

    assert service.getOffset(new TopicPartition(topic, partition)) == recordCount;

    service.closeAll();
    // don't drop pipe in current version
    // assert !conn.pipeExist(pipe);
  }

  @Test
  public void testNativeAvroInputIngestion() throws Exception
  {
    // avro
    SchemaBuilder schemaBuilder = SchemaBuilder.struct()
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
      .field("int8Optional", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
      .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
      .field("mapArrayMapInt", SchemaBuilder.map(Schema.STRING_SCHEMA,
        SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()).build()).build());
    Struct original = new Struct(schemaBuilder.build())
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
      .put("mapArrayMapInt", Collections.singletonMap("field",
        Arrays.asList(Collections.singletonMap("field", 1), Collections.singletonMap("field", 1))));


    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
    byte[] converted = avroConverter.fromConnectData(topic, original.schema(), original);
    SchemaAndValue avroInputValue = avroConverter.toConnectData(topic, converted);

    avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), true);
    converted = avroConverter.fromConnectData(topic, original.schema(), original);
    SchemaAndValue avroInputKey = avroConverter.toConnectData(topic, converted);

    long startOffset = 0;
    long endOffset = 2;
    long recordCount = endOffset + 1;

    SinkRecord avroRecordValue = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", avroInputValue.schema(), avroInputValue.value(), startOffset);

    SinkRecord avroRecordKey = new SinkRecord(topic, partition, avroInputKey.schema(), avroInputKey.value(),
      Schema.STRING_SCHEMA, "test", startOffset + 1);

    SinkRecord avroRecordKeyValue = new SinkRecord(topic, partition, avroInputKey.schema(), avroInputKey.value(),
      avroInputKey.schema(), avroInputKey.value(), startOffset + 2);

    conn.createTable(table);
    conn.createStage(stage);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(recordCount)
        .addTask(table, topic, partition)
        .build();

    service.insert(avroRecordValue);
    service.insert(avroRecordKey);
    service.insert(avroRecordKeyValue);

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
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == recordCount, 30, 20);

    //change cleaner
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition) == 0,30, 20);

    assert service.getOffset(new TopicPartition(topic, partition)) == recordCount;

    service.closeAll();
  }

  @Test
  public void testNativeBrokenIngestion() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);

    // Mismatched schema and value
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");

    long startOffset = 0;
    long endOffset = 2;
    long recordCount = endOffset + 1;

    SinkRecord brokenValue = new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "test",
      brokenInputValue.schema(), brokenInputValue.value(), startOffset);

    SinkRecord brokenKey = new SinkRecord(topic, partition, brokenInputValue.schema(), brokenInputValue.value(),
      Schema.STRING_SCHEMA, "test", startOffset + 1);

    SinkRecord brokenKeyValue = new SinkRecord(topic, partition, brokenInputValue.schema(), brokenInputValue.value(),
      brokenInputValue.schema(), brokenInputValue.value(), startOffset + 2);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(recordCount)
        .addTask(table, topic, partition)
        .build();

    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(brokenKeyValue);

    List<String> files = conn.listStage(table, "", true);
    // two files per broken record
    assert files.size() == 6;
    String name = files.get(0);
    assert TestUtils.getPartitionFromBrokenFileName(name) == partition;

  }

  @Test
  public void testNativeNullIngestion() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);

    // Mismatched schema and value
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");
    SchemaAndValue correctInputValue = new SchemaAndValue(Schema.STRING_SCHEMA, "correct");

    long recordCount = 1;

    SinkRecord brokenValue = new SinkRecord(topic, partition, null, null,
      brokenInputValue.schema(), brokenInputValue.value(), 0);

    SinkRecord brokenKey = new SinkRecord(topic, partition, brokenInputValue.schema(), brokenInputValue.value(),
      null, null, 1);

    SinkRecord correctValue = new SinkRecord(topic, partition, null, null,
      correctInputValue.schema(), correctInputValue.value(), 2);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(recordCount)
        .addTask(table, topic, partition)
        .build();

    service.insert(brokenValue);
    service.insert(brokenKey);
    service.insert(correctValue);

    List<String> files = conn.listStage(table, "", true);
    assert files.size() == 2;
    String name = files.get(0);
    assert TestUtils.getPartitionFromBrokenFileName(name) == partition;

    TestUtils.assertWithRetry(() ->
        conn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
          table, partition)).size() == 1,
      5, 4);
    service.callAllGetOffset();
    files = conn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
      table, partition));
    String fileName = files.get(0);

    assert FileNameUtils.fileNameToTimeIngested(fileName) < System.currentTimeMillis();
    assert FileNameUtils.fileNameToPartition(fileName) == partition;
    assert FileNameUtils.fileNameToStartOffset(fileName) == 2;
    assert FileNameUtils.fileNameToEndOffset(fileName) == 2;

    //wait for ingest
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == recordCount, 30, 20);

    //change cleaner
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, partition) == 0,30, 20);

    assert service.getOffset(new TopicPartition(topic, partition)) == 3;

    service.closeAll();

  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testNativeNullValueIngestion() throws Exception
  {
    long recordCount = 1;

    SinkRecord brokenValue = new SinkRecord(topic, partition, null, null, null, null, 0);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(recordCount)
        .addTask(table, topic, partition)
        .build();

    service.insert(brokenValue);
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
      }, 30, 20);

    service.closeAll();
  }

  private void insert(SnowflakeSinkService sink, int partition,
                                 int numOfRecord)
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

  @Test
  public void testFileSize() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    int numOfRecord = 111; //181 bytes each
    int recordSize = 180;
    long size = 10000;

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setFileSize(size)
        .setFlushTime(10)
        .addTask(table, topic, partition)
        .build();

    insert(service, partition, numOfRecord);

    int a = getStageSize(stage, table, partition);

    assert getStageSize(stage, table, partition) == numOfRecord * recordSize / size;

    // time based flush
    TestUtils.assertWithRetry(
        () -> {
          service.insert(new ArrayList<>());
          return getStageSize(stage, table, partition) == numOfRecord * recordSize / size + 1;
        }, 10, 10);

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

    assert getStageSize(stage, table, partition) == 0;

    TestUtils.assertWithRetry(() -> {
        service.insert(new ArrayList<>()); // trigger time based flush
        return getStageSize(stage, table, partition) == 1;
      }, 15, 4);

    service.closeAll();
  }

  @Test
  public void testSinkServiceNegative()
  {
    conn.createTable(table);
    conn.createStage(stage);
    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .setRecordNumber(1)
        .build();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    service.getOffset(topicPartition);
    List<TopicPartition> topicPartitionList = new ArrayList<>();
    topicPartitionList.add(topicPartition);
    service.close(topicPartitionList);

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input = converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    service.insert(
      new SinkRecord(topic, partition, null, null, input.schema(), input.value(), 0)
    );
    service.startTask(table, topic, partition);
  }

  @Test
  public void testRecoverReprocessFiles() throws Exception
  {
    String data = "{\"content\":{\"name\":\"test\"},\"meta\":{\"offset\":0," +
      "\"topic\":\"test\",\"partition\":0}}";

    // Two hours ago                         h   m    s    milli
    long time = System.currentTimeMillis() - 2 * 60 * 60 * 1000L;

    String fileName1 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 0, 0, time);
    String fileName2 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 1, 1, time);
    String fileName3 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 2, 3, time);
    String fileName4 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME,
      table, 0, 4, 5, time);

    conn.createStage(stage);
    conn.createTable(table);
    conn.createPipe(table, stage, pipe);

    SnowflakeIngestionService ingestionService =
      conn.buildIngestService(stage, pipe);

    // File 1 is successfully ingested (ingest history can find this file, so removed)
    // File 2 is not ingested, so moved to table stage
    // File 3 is not ingested, so moved to table stage
    // File 4 is removed by reprocess cleaner
    conn.put(stage, fileName1, data);
    conn.put(stage, fileName2, data);
    conn.put(stage, fileName3, data);
    conn.put(stage, fileName4, data);

    ingestionService.ingestFile(fileName1);

    assert getStageSize(stage, table, 0) == 4;

    SnowflakeSinkService service = SnowflakeSinkServiceFactory.builder(conn)
      .addTask(table, topic, partition)
      .setRecordNumber(1) // immediate flush
      .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result = converter.toConnectData(topic, "12321".getBytes(StandardCharsets.UTF_8));
    // This record is ingested as well.
    SinkRecord record = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", result.schema(), result.value(), 3);
    // lazy init and recovery function
    service.insert(record);
    // wait for async put
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, 0) == 5, 5, 10);
    // call snow pipe
    service.callAllGetOffset();
    // cleaner will remove previous files and ingested new file
    TestUtils.assertWithRetry(() -> getStageSize(stage, table, 0) == 0, 30, 10);

    // verify that filename2 appears in table stage
    List<String> files = conn.listStage(table, "", true);
    assert files.size() == 2;

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
    assert files.size() == 2;
    String name = files.get(0);
    assert TestUtils.getPartitionFromBrokenFileName(name) == partition;
    assert TestUtils.getOffsetFromBrokenFileName(name) == offset;

    service.closeAll();
  }

  int getStageSize(String stage, String table, int partition)
  {
    return conn.listStage(stage,
      FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME, table, partition)).size();
  }

  /**
   * Test whether cleaner can recover from network exceptions.
   * @throws Exception
   */
  @Test
  public void testCleanerRecover() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    SnowflakeConnectionService spyConn = spy(conn);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(spyConn)
        .setRecordNumber(1)
        .addTask(table, topic, partition)
        .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input = converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record1 = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", input.schema(), input.value(), offset);

    service.insert(record1);
    TestUtils.assertWithRetry(() -> spyConn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
      table, partition)).size() == 1,
      5, 4);

    // ingest files
    service.callAllGetOffset();

    System.out.println("break connection");
    doThrow(SnowflakeErrors.ERROR_2001.getException()).when(spyConn).purgeStage(anyString(), anyList());
    // Sleep 6 minutes so that cleaner encounters 6 exceptions. Just to make sure cleaner restart is triggered
    Thread.sleep(6 * 60 * 1000);

    System.out.println("recover connection");
    doCallRealMethod().when(spyConn).purgeStage(anyString(), anyList());

    // Sleep 4 minutes. Total sleep time is 10 minutes to test read ingestHistory
    Thread.sleep(4 * 60 * 1000);

    TestUtils.assertWithRetry(() -> spyConn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
      table, partition)).size() == 0,
      30, 8);
  }

  /**
   * This test is ignored because it is tested manually.
   * Need to check snowflake query history to verify that there are only
   * list stage commands every minutes for each pipe.
   */
  @Ignore
  @Test
  public void testCleanerRecoverListCount() throws Exception
  {
    conn.createTable(table);
    conn.createStage(stage);
    SnowflakeConnectionService spyConn = spy(conn);

    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(spyConn)
        .setRecordNumber(1)
        .addTask(table, topic, partition)
        .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input = converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record1 = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", input.schema(), input.value(), offset);

    service.insert(record1);
    TestUtils.assertWithRetry(() -> spyConn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
      table, partition)).size() == 1,
      5, 4);

    System.out.println("break connection");
    service.callAllGetOffset();
    doThrow(SnowflakeErrors.ERROR_2001.getException()).when(spyConn).purgeStage(anyString(), anyList());
    Thread.sleep(120000);

    System.out.println("recover connection");
    doCallRealMethod().when(spyConn).purgeStage(anyString(), anyList());

    // count how many list statement are here
    Thread.sleep(1200000);

    TestUtils.assertWithRetry(() -> spyConn.listStage(stage, FileNameUtils.filePrefix(TestUtils.TEST_CONNECTOR_NAME,
      table, partition)).size() == 0,
      60, 8);
  }
}
