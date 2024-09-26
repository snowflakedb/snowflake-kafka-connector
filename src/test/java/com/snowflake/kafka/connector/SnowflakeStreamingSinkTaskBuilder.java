package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2.partitionChannelKey;

import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceV1;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.BufferedTopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import com.snowflake.kafka.connector.internal.streaming.StreamingBufferThreshold;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.streaming.iceberg.IcebergInitService;
import com.snowflake.kafka.connector.streaming.iceberg.IcebergTableSchemaValidator;
import java.util.Collections;
import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * A builder for SnowflakeSinkTask unit testing. Right now supports only Snowpipe Streaming with
 * schematization but can be easily extended if needed.
 */
public class SnowflakeStreamingSinkTaskBuilder {

  private final String topicName;
  private final int partition;
  private final TopicPartition topicPartition;
  private Map<String, String> config = defaultConfig();
  private SnowflakeStreamingIngestClient streamingIngestClient = defaultStreamingIngestClient();
  private KafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

  public static SnowflakeStreamingSinkTaskBuilder create(String topicName, int partition) {
    return new SnowflakeStreamingSinkTaskBuilder(topicName, partition);
  }

  public SnowflakeSinkTask build() {
    SnowflakeConnectionService mockConnectionService =
        Mockito.mock(SnowflakeConnectionServiceV1.class);
    SnowflakeSinkServiceV2 mockSinkService =
        streamingSinkService(config, streamingIngestClient, errorReporter, mockConnectionService);
    return new SnowflakeSinkTask(mockSinkService, mockConnectionService);
  }

  public SnowflakeStreamingSinkTaskBuilder withStreamingIngestClient(
      SnowflakeStreamingIngestClient streamingIngestClient) {
    this.streamingIngestClient = streamingIngestClient;
    return this;
  }

  public SnowflakeStreamingSinkTaskBuilder withErrorReporter(
      KafkaRecordErrorReporter errorReporter) {
    this.errorReporter = errorReporter;
    return this;
  }

  private SnowflakeStreamingSinkTaskBuilder(String topicName, int partition) {
    this.topicName = topicName;
    this.partition = partition;
    this.topicPartition = new TopicPartition(topicName, partition);
  }

  private Map<String, String> defaultConfig() {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(BUFFER_COUNT_RECORDS, "1"); // override
    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    config.put(ENABLE_SCHEMATIZATION_CONFIG, Boolean.TRUE.toString());
    return config;
  }

  private SnowflakeSinkServiceV2 streamingSinkService(
      Map<String, String> config,
      SnowflakeStreamingIngestClient mockStreamingClient,
      KafkaRecordErrorReporter errorReporter,
      SnowflakeConnectionService mockConnectionService) {
    SnowflakeTelemetryService mockTelemetryService = Mockito.mock(SnowflakeTelemetryService.class);
    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));
    IcebergTableSchemaValidator mockIcebergTableSchemaValidator =
        Mockito.mock(IcebergTableSchemaValidator.class);
    IcebergInitService mockIcebergInitService = Mockito.mock(IcebergInitService.class);

    TopicPartitionChannel topicPartitionChannel =
        new BufferedTopicPartitionChannel(
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

    return new SnowflakeSinkServiceV2(
        1,
        10 * 1024 * 1024,
        1,
        mockConnectionService,
        new RecordService(),
        mockTelemetryService,
        mockIcebergTableSchemaValidator,
        mockIcebergInitService,
        null,
        SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT,
        true,
        errorReporter,
        inMemorySinkTaskContext,
        mockStreamingClient,
        config,
        /*enableSchematization=*/ false,
        /*closeChannelsInParallel=*/ true,
        topicPartitionChannelMap);
  }

  private SnowflakeStreamingIngestClient defaultStreamingIngestClient() {
    SnowflakeStreamingIngestClient mockStreamingClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    SnowflakeStreamingIngestChannel mockStreamingChannel =
        Mockito.mock(SnowflakeStreamingIngestChannel.class);

    InsertValidationResponse validationResponse1 = new InsertValidationResponse();

    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse1);

    Mockito.when(mockStreamingClient.openChannel(ArgumentMatchers.any(OpenChannelRequest.class)))
        .thenReturn(mockStreamingChannel);

    return mockStreamingClient;
  }
}
