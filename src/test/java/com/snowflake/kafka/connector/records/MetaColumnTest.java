package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.mock.MockSchemaRegistryClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class MetaColumnTest
{
  private static String META = "meta";
  private static String KEY = "key";
  private static final String TEST_VALUE_FILE_NAME = "test.avro";

  private String topic = "test";
  private int partition = 0;
  private ObjectMapper mapper = new ObjectMapper();

  // initialize config maps to test metadata
  private HashMap<String, String> createTimeConfig = new HashMap<String, String>()
  {
    {
      put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME, "false");
    }
  };
  private HashMap<String, String> topicConfig = new HashMap<String, String>()
  {
    {
      put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC, "false");
    }
  };
  private HashMap<String, String> offsetAndPartitionConfig = new HashMap<String, String>()
  {
    {
      put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "false");
    }
  };
  private HashMap<String, String> allConfig = new HashMap<String, String>()
  {
    {
      put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL, "false");
    }
  };

  @Test
  public void testKey() throws IOException {
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    RecordService service = new RecordService();
    SchemaAndValue input = converter.toConnectData(topic, ("{\"name\":\"test" +
      "\"}").getBytes(StandardCharsets.UTF_8));
    long timestamp = System.currentTimeMillis();

    //no timestamp
    SinkRecord record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "test",
        input.schema(), input.value(), 0, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE);

    String output = service.processRecord(record);

    JsonNode result = mapper.readTree(output);

    assert result.get(META).has(KEY);
    assert result.get(META).get(KEY).asText().equals("test");

    System.out.println(result.toString());
  }

  @Test
  public void testConfig() throws IOException {
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    RecordService service = new RecordService();
    SchemaAndValue input = converter.toConnectData(topic, ("{\"name\":\"test" +
      "\"}").getBytes(StandardCharsets.UTF_8));
    long timestamp = System.currentTimeMillis();

    SinkRecord record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "test",
        input.schema(), input.value(), 0, timestamp,
        TimestampType.CREATE_TIME);

    // test metadata configuration -- remove topic
    SnowflakeMetadataConfig metadataConfig = new SnowflakeMetadataConfig(topicConfig);
    service.setMetadataConfig(metadataConfig);
    JsonNode result = mapper.readTree(service.processRecord(record));
    assert result.has(META);
    assert !result.get(META).has(RecordService.TOPIC);
    assert result.get(META).has(RecordService.OFFSET);
    assert result.get(META).has(RecordService.PARTITION);
    assert result.get(META).has(record.timestampType().name);

    // test metadata configuration -- remove offset and partition
    metadataConfig = new SnowflakeMetadataConfig(offsetAndPartitionConfig);
    service.setMetadataConfig(metadataConfig);
    result = mapper.readTree(service.processRecord(record));
    assert result.has(META);
    assert !result.get(META).has(RecordService.OFFSET);
    assert !result.get(META).has(RecordService.PARTITION);
    assert result.get(META).has(record.timestampType().name);
    assert result.get(META).has(RecordService.TOPIC);

    // test metadata configuration -- remove time stamp
    metadataConfig = new SnowflakeMetadataConfig(createTimeConfig);
    service.setMetadataConfig(metadataConfig);
    result = mapper.readTree(service.processRecord(record));
    assert result.has(META);
    assert !result.get(META).has(record.timestampType().name);
    assert result.get(META).has(RecordService.TOPIC);
    assert result.get(META).has(RecordService.OFFSET);
    assert result.get(META).has(RecordService.PARTITION);

    // test metadata configuration -- remove all
    metadataConfig = new SnowflakeMetadataConfig(allConfig);
    service.setMetadataConfig(metadataConfig);
    result = mapper.readTree(service.processRecord(record));
    assert !result.has(META);

    System.out.println("Config test success");
  }

  @Test
  public void testTimeStamp() throws IOException
  {
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    RecordService service = new RecordService();
    SchemaAndValue input = converter.toConnectData(topic, ("{\"name\":\"test" +
      "\"}").getBytes(StandardCharsets.UTF_8));
    long timestamp = System.currentTimeMillis();

    //no timestamp
    SinkRecord record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "test",
        input.schema(), input.value(), 0, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE);

    String output = service.processRecord(record);

    JsonNode result = mapper.readTree(output);

    assert !result.get(META).has(TimestampType.CREATE_TIME.name);
    assert !result.get(META).has(TimestampType.LOG_APPEND_TIME.name);

    //create time
    record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "test",
        input.schema(), input.value(), 0, timestamp,
        TimestampType.CREATE_TIME);

    output = service.processRecord(record);
    result = mapper.readTree(output);

    assert result.get(META).has(TimestampType.CREATE_TIME.name);
    assert result.get(META).get(TimestampType.CREATE_TIME.name).asLong() == timestamp;

    //log append time
    record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "test",
        input.schema(), input.value(), 0, timestamp,
        TimestampType.LOG_APPEND_TIME);

    output = service.processRecord(record);
    result = mapper.readTree(output);

    assert result.get(META).has(TimestampType.LOG_APPEND_TIME.name);
    assert result.get(META).get(TimestampType.LOG_APPEND_TIME.name).asLong() == timestamp;
  }

  @Test
  public void testSchemaID() throws IOException
  {
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input = converter.toConnectData(topic, ("{\"name\":\"test" +
      "\"}").getBytes(StandardCharsets.UTF_8));

    //no schema id
    SinkRecord record = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", input.schema(), input.value(), 0);
    SnowflakeRecordContent content = (SnowflakeRecordContent) record.value();

    assert content.getSchemaID() == SnowflakeRecordContent.NON_AVRO_SCHEMA;

    //broken data
    input = converter.toConnectData(topic, ("123adsada").getBytes(StandardCharsets.UTF_8));
    record = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", input.schema(), input.value(), 0);
    content = (SnowflakeRecordContent) record.value();

    assert content.getSchemaID() == SnowflakeRecordContent.NON_AVRO_SCHEMA;

    //avro without schema registry
    converter = new SnowflakeAvroConverterWithoutSchemaRegistry();
    URL resource = ConverterTest.class.getResource(TEST_VALUE_FILE_NAME);
    byte[] testFile = Files.readAllBytes(Paths.get(resource.getFile()));
    input = converter.toConnectData(topic, testFile);
    record = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", input.schema(), input.value(), 0);
    content = (SnowflakeRecordContent) record.value();

    assert content.getSchemaID() == SnowflakeRecordContent.NON_AVRO_SCHEMA;

    //include schema id
    MockSchemaRegistryClient client = new MockSchemaRegistryClient();
    converter = new SnowflakeAvroConverter();
    ((SnowflakeAvroConverter) converter).setSchemaRegistry(client);
    input = converter.toConnectData(topic, client.getData());
    record = new SinkRecord(topic, partition, Schema.STRING_SCHEMA
      , "test", input.schema(), input.value(), 0);
    content = (SnowflakeRecordContent) record.value();
    assert content.getSchemaID() == 1;
  }
}
