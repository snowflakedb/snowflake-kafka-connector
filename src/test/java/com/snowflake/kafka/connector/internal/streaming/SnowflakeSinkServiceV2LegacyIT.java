package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceV2LegacyIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();

  @BeforeEach
  public void setup() {
    conn.createTable(table);
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(table);
  }

  @Test
  @Disabled(
      "This test is flaky. Runs locally. Instead of time based waits it should be reimplemented to"
          + " check if proper lag has been set in overrideProperties in Ingest SDK client")
  public void testStreamingIngestionValidClientLag() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "7");

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

    final long noOfRecords = 50;
    List<SinkRecord> sinkRecords =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, partition);

    service.insert(sinkRecords);

    // Wait 5 seconds here and no flush should happen since the max client lag is 7 seconds
    Thread.sleep(5 * 1000);
    Assertions.assertEquals(
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, service.getOffset(topicPartition));

    // Wait for enough time, the rows should be flushed
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == noOfRecords, 3, 5);

    service.closeAll();
  }
}
