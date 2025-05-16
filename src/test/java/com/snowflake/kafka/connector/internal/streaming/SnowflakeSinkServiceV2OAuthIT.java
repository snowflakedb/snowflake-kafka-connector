package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceV2OAuthIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getOAuthConnectionServiceForStreaming();

  @BeforeEach
  public void setup() {
    conn.createTable(table);
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(table);
  }

  @Test
  void shouldIngestDataWithOAuth() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreamingWithOAuth();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

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
  }
}
