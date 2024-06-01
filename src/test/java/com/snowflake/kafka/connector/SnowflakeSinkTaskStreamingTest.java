package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2.partitionChannelKey;

import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceV1;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import com.snowflake.kafka.connector.internal.streaming.StreamingBufferThreshold;
import com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/** Unit test for testing Snowflake Sink Task Behavior with Snowpipe Streaming */
public class SnowflakeSinkTaskStreamingTest {
  private String topicName;
  private static int partition = 0;
  private TopicPartition topicPartition;

  @Before
  public void setup() {
    topicName = TestUtils.randomTableName();
    topicPartition = new TopicPartition(topicName, partition);
  }

  @Test
  public void testSinkTaskInvalidRecord_InMemoryDLQ() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(BUFFER_COUNT_RECORDS, "1"); // override
    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    SnowflakeConnectionService mockConnectionService =
        Mockito.mock(SnowflakeConnectionServiceV1.class);
    SnowflakeStreamingIngestClient mockStreamingClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    SnowflakeTelemetryService mockTelemetryService = Mockito.mock(SnowflakeTelemetryService.class);
    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));
    SnowflakeStreamingIngestChannel mockStreamingChannel =
        Mockito.mock(SnowflakeStreamingIngestChannel.class);
    InsertValidationResponse validationResponse1 = new InsertValidationResponse();
    InsertValidationResponse.InsertError insertError =
        new InsertValidationResponse.InsertError("CONTENT", 0);
    insertError.setException(new SFException(ErrorCode.INVALID_VALUE_ROW, "INVALID_CHANNEL"));
    validationResponse1.addError(insertError);

    Mockito.when(mockStreamingClient.openChannel(ArgumentMatchers.any(OpenChannelRequest.class)))
        .thenReturn(mockStreamingChannel);

    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse1);
    Mockito.when(mockConnectionService.getConnectorName()).thenReturn(TEST_CONNECTOR_NAME);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            SnowflakeSinkServiceV2.partitionChannelKey(topicName, partition),
            topicName,
            new StreamingBufferThreshold(10, 10_000, 1),
            config,
            errorReporter,
            inMemorySinkTaskContext,
            mockConnectionService,
            mockTelemetryService);

    Map topicPartitionChannelMap =
        Collections.singletonMap(partitionChannelKey(topicName, partition), topicPartitionChannel);

    SnowflakeSinkServiceV2 mockSinkService =
        new SnowflakeSinkServiceV2(
            1,
            10 * 1024 * 1024,
            1,
            mockConnectionService,
            new RecordService(),
            mockTelemetryService,
            null,
            SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT,
            true,
            errorReporter,
            inMemorySinkTaskContext,
            mockStreamingClient,
            config,
            false,
            topicPartitionChannelMap);

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask(mockSinkService, mockConnectionService);

    // Inits the sinktaskcontext
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topicName, partition));

    // send bad data
    SchemaAndValue brokenInputValue = new SchemaAndValue(Schema.INT32_SCHEMA, 12);
    SinkRecord brokenValue =
        new SinkRecord(
            topicName,
            partition,
            null,
            null,
            brokenInputValue.schema(),
            brokenInputValue.value(),
            0);
    sinkTask.put(Collections.singletonList(brokenValue));

    // commit offset
    final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(1));

    Assert.assertEquals(1, errorReporter.getReportedRecords().size());
  }
}
