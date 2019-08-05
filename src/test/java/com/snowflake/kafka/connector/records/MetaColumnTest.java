package com.snowflake.kafka.connector.records;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;

public class MetaColumnTest
{
  private static String META = "meta";

  private String topic = "test";
  private int partition = 0;
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testTimeStamp() throws IOException
  {
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    RecordService service = new RecordService();
    SchemaAndValue input = converter.toConnectData(topic, ("{\"name\":\"test" +
      "\"}").getBytes());
    long timestamp = System.currentTimeMillis();

    //no timestamp
    SinkRecord record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "test",
        input.schema(), input.value(), 0, timestamp,
        TimestampType.NO_TIMESTAMP_TYPE);

    String output =  service.processRecord(record);

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
}
