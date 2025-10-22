package com.snowflake.kafka.connector.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingSinkServiceBuilder;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MetaColumnIT {
  private final String topic = "test";
  private final int partition = 0;
  private final String tableName = TestUtils.randomTableName();
  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
  private final ObjectMapper mapper = new ObjectMapper();

  @AfterEach
  void afterEach() {
    TestUtils.dropTable(tableName);
  }

  @Test
  void testKey() throws Exception {
    conn.createTable(tableName);

    Map<String, String> config = TestUtils.getConfForStreaming();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(tableName, topicPartition);

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result =
        converter.toConnectData(topic, ("{\"name\":\"test\"}").getBytes(StandardCharsets.UTF_8));
    SinkRecord record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "key1", result.schema(), result.value(), 0);

    service.insert(record);

    record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "key2", result.schema(), result.value(), 1);

    service.insert(record);

    record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "key3", result.schema(), result.value(), 2);

    service.insert(record);

    TestUtils.assertWithRetry(
        () -> {
          service.callAllGetOffset();
          ResultSet resultSet = TestUtils.executeQuery("select RECORD_METADATA from " + tableName);

          boolean hasKey1 = false;
          boolean hasKey3 = false;

          for (int i = 0; i < 3; i++) {
            if (!resultSet.next()) {
              return false;
            }
            JsonNode node = mapper.readTree(resultSet.getString(1));
            if (node.has("key")) {
              if (node.get("key").asText().equals("key1")) {
                hasKey1 = true;
              } else if (node.get("key").asText().equals("key3")) {
                hasKey3 = true;
              }
            }
          }
          if (resultSet.next() || !hasKey1 || !hasKey3) {
            return false;
          }
          return true;
        },
        30,
        15);

    service.closeAll();
  }
}

