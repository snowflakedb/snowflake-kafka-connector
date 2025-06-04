package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class StreamingRecordMapperTest {

  protected static final ObjectMapper objectMapper = new ObjectMapper();

  protected static final String fullMetadataJsonExample =
      "{"
          + "\"offset\": 10,"
          + "\"topic\": \"topic\","
          + "\"partition\": 0,"
          + "\"key\": \"key\","
          + "\"schema_id\": 1,"
          + "\"key_schema_id\": 2,"
          + "\"CreateTime\": 3,"
          + "\"LogAppendTime\": 4,"
          + "\"SnowflakeConnectorPushTime\": 5,"
          + "\"headers\": {\"objectAsJsonStringHeader\": {"
          + "\"key1\": \"value1\","
          + "\"key2\": \"value2\""
          + "},"
          + "\"header2\": \"testheaderstring\","
          + "\"header3\": 3.5}"
          + "}";

  protected static final String fullMetadataJsonExampleWithoutWhitespace =
      fullMetadataJsonExample.replaceAll("\\s+", "");

  protected static final String fullMetadataWithoutWhitespace =
      fullMetadataJsonExample.replaceAll("\\s+", "");

  protected static final MetadataRecord fullRecordMetadata =
      new MetadataRecord(
          10L,
          "topic",
          0,
          "key",
          1,
          2,
          3L,
          4L,
          5L,
          Map.of(
              "header3",
              "3.5",
              "header2",
              "testheaderstring",
              "objectAsJsonStringHeader",
              "{\"key1\":\"value1\",\"key2\":\"value2\"}"));

  protected static RecordService.SnowflakeTableRow buildRow(String content)
      throws JsonProcessingException {
    return buildRow(content, fullMetadataJsonExample);
  }

  protected static RecordService.SnowflakeTableRow buildRow(String content, String metadata)
      throws JsonProcessingException {
    return new RecordService.SnowflakeTableRow(
        new SnowflakeRecordContent(objectMapper.readTree(content)),
        objectMapper.readTree(metadata));
  }
}
