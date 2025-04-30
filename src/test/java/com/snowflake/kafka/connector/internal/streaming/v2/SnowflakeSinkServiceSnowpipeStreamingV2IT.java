package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingSinkServiceBuilder;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceSnowpipeStreamingV2IT {

  private String appName;
  private final String table = TestUtils.randomTableName();
  private final int partition = 0;
  private final String topic = table;
  private final TopicPartition topicPartition = new TopicPartition(topic, partition);

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(table);
    TestUtils.dropPipe(PipeNameProvider.pipeName(appName, table));
  }

  @Test
  void shouldInsertSingleRecordWithoutSchematization() {
    // given
    Map<String, String> config = TestUtils.getConfForStreamingV2();
    appName = config.get(Utils.NAME);

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test_key" + offset,
            input.schema(),
            input.value(),
            offset);

    SnowflakeConnectionService connectionService = TestUtils.getConnectionServiceForStreaming();
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(connectionService, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();

    // when, then
    assertThat(service.getOffset(topicPartition))
        .isEqualTo(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

    // when
    service.insert(record);

    // then
    await()
        .timeout(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> service.getOffset(topicPartition) == 1);
  }

  @Test
  void shouldSendBrokenRecordsToDLQ() {
    // given
    Map<String, String> config = TestUtils.getConfForStreamingV2();
    appName = config.get(Utils.NAME);

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    SnowflakeConnectionService connectionService = TestUtils.getConnectionServiceForStreaming();
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(connectionService, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();

    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, "error");

    long startOffset = 0;

    SinkRecord brokenRecord =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            brokenInputValue.schema(),
            brokenInputValue.value(),
            startOffset);

    // when
    service.insert(brokenRecord);

    // then
    assertThat(errorReporter.getReportedRecords()).hasSize(1);
    assertThat(service.getOffset(topicPartition))
        .isEqualTo(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);
  }
}
