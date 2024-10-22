package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordServiceFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class BufferedTopicPartitionChannelTest {

  @Mock private KafkaRecordErrorReporter mockKafkaRecordErrorReporter;

  @Mock private SnowflakeStreamingIngestClient mockStreamingClient;

  @Mock private SnowflakeStreamingIngestChannel mockStreamingChannel;

  @Mock private SinkTaskContext mockSinkTaskContext;

  @Mock private SnowflakeConnectionService mockSnowflakeConnectionService;

  @Mock private SnowflakeTelemetryService mockTelemetryService;

  @Mock private SchemaEvolutionService schemaEvolutionService;

  private static final String TOPIC = "TEST";

  private static final int PARTITION = 0;

  private static final String TEST_CHANNEL_NAME =
      SnowflakeSinkServiceV2.partitionChannelKey(TOPIC, PARTITION);
  private static final String TEST_TABLE_NAME = "TEST_TABLE";

  private TopicPartition topicPartition;

  private Map<String, String> sfConnectorConfig;

  private BufferThreshold streamingBufferThreshold;

  private SFException SF_EXCEPTION = new SFException(ErrorCode.INVALID_CHANNEL, "INVALID_CHANNEL");

  private final boolean enableSchematization;

  public BufferedTopicPartitionChannelTest(boolean enableSchematization) {
    this.enableSchematization = enableSchematization;
  }

  @Parameterized.Parameters(name = "enableSchematization: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(
        new Object[][] {
          {true}, {false},
        });
  }

  @Before
  public void setupEachTest() {
    mockStreamingClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    mockStreamingChannel = Mockito.mock(SnowflakeStreamingIngestChannel.class);
    mockKafkaRecordErrorReporter = Mockito.mock(KafkaRecordErrorReporter.class);
    mockSinkTaskContext = Mockito.mock(SinkTaskContext.class);
    mockSnowflakeConnectionService = Mockito.mock(SnowflakeConnectionService.class);
    mockTelemetryService = Mockito.mock(SnowflakeTelemetryService.class);
    Mockito.when(mockStreamingClient.isClosed()).thenReturn(false);
    Mockito.when(mockStreamingClient.openChannel(ArgumentMatchers.any(OpenChannelRequest.class)))
        .thenReturn(mockStreamingChannel);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);
    this.topicPartition = new TopicPartition(TOPIC, PARTITION);
    this.sfConnectorConfig = TestUtils.getConfig();
    this.streamingBufferThreshold = new StreamingBufferThreshold(1, 10_000, 1);
    this.sfConnectorConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
        Boolean.toString(this.enableSchematization));
  }

  /* Valid response but has errors, error tolerance is ALL. Meaning it will ignore the error.  */
  @Test
  public void testInsertRows_ValidationResponseHasErrors_ErrorTolerance_ALL_LogEnableTrue()
      throws Exception {
    InsertValidationResponse validationResponse = new InsertValidationResponse();
    InsertValidationResponse.InsertError insertErrorWithException =
        new InsertValidationResponse.InsertError("CONTENT", 0);
    insertErrorWithException.setException(SF_EXCEPTION);
    validationResponse.addError(insertErrorWithException);
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    Map<String, String> sfConnectorConfigWithErrors = new HashMap<>(sfConnectorConfig);
    sfConnectorConfigWithErrors.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    sfConnectorConfigWithErrors.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    sfConnectorConfigWithErrors.put(ERRORS_LOG_ENABLE_CONFIG, "true");

    InMemoryKafkaRecordErrorReporter kafkaRecordErrorReporter =
        new InMemoryKafkaRecordErrorReporter();
    BufferedTopicPartitionChannel topicPartitionChannel =
        new BufferedTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfigWithErrors,
            kafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            schemaEvolutionService,
            new InsertErrorMapper());

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    BufferedTopicPartitionChannel.StreamingBuffer streamingBuffer =
        topicPartitionChannel.new StreamingBuffer();
    streamingBuffer.insert(records.get(0));

    assert topicPartitionChannel.insertRecords(streamingBuffer).hasErrors();

    assert kafkaRecordErrorReporter.getReportedRecords().size() == 1;
  }

  /* SFExceptions is thrown in first attempt of insert rows. It is also thrown while refetching committed offset from snowflake after reopening the channel */
  @Test(expected = SFException.class)
  public void testInsertRows_GetOffsetTokenFailureAfterReopenChannel() throws Exception {
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION);

    // Send exception in fallback (i.e after reopen channel)
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(SF_EXCEPTION);

    BufferedTopicPartitionChannel topicPartitionChannel =
        new BufferedTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            schemaEvolutionService,
            new InsertErrorMapper());

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    try {
      BufferedTopicPartitionChannel.StreamingBuffer streamingBuffer =
          topicPartitionChannel.new StreamingBuffer();
      streamingBuffer.insert(records.get(0));
      topicPartitionChannel.insertRecords(streamingBuffer);
    } catch (SFException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(2)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .insertRows(
              ArgumentMatchers.any(Iterable.class),
              ArgumentMatchers.any(String.class),
              ArgumentMatchers.any(String.class));
      // get offset token is called once after channel re-open
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* Runtime exception does not perform any fallbacks. */
  @Test(expected = RuntimeException.class)
  public void testInsertRows_RuntimeException() throws Exception {
    RuntimeException exception = new RuntimeException("runtime exception");
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenThrow(exception);

    BufferedTopicPartitionChannel topicPartitionChannel =
        new BufferedTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            schemaEvolutionService,
            new InsertErrorMapper());

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    topicPartitionChannel.insertRecord(records.get(0), true);

    try {
      topicPartitionChannel.insertRecords(topicPartitionChannel.getStreamingBuffer());
    } catch (RuntimeException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .insertRows(
              ArgumentMatchers.any(Iterable.class),
              ArgumentMatchers.any(String.class),
              ArgumentMatchers.any(String.class));
      throw ex;
    }
  }

  /* Valid response but has errors. */
  @Test(expected = DataException.class)
  public void testInsertRows_ValidationResponseHasErrors_NoErrorTolerance() throws Exception {
    InsertValidationResponse validationResponse = new InsertValidationResponse();
    InsertValidationResponse.InsertError insertErrorWithException =
        new InsertValidationResponse.InsertError("CONTENT", 0);
    insertErrorWithException.setException(SF_EXCEPTION);
    validationResponse.addError(insertErrorWithException);
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);
    Mockito.doNothing()
        .when(mockTelemetryService)
        .reportKafkaConnectFatalError(ArgumentMatchers.anyString());

    BufferedTopicPartitionChannel topicPartitionChannel =
        new BufferedTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            false,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            schemaEvolutionService,
            new InsertErrorMapper());

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    topicPartitionChannel.insertRecord(records.get(0), true);

    try {
      topicPartitionChannel.insertRecords(topicPartitionChannel.getStreamingBuffer());
    } catch (DataException ex) {
      throw ex;
    }
  }

  /* Valid response but has errors, error tolerance is ALL. Meaning it will ignore the error.  */
  @Test
  public void testInsertRows_ValidationResponseHasErrors_ErrorTolerance_ALL() throws Exception {
    InsertValidationResponse validationResponse = new InsertValidationResponse();
    InsertValidationResponse.InsertError insertErrorWithException =
        new InsertValidationResponse.InsertError("CONTENT", 0);
    insertErrorWithException.setException(SF_EXCEPTION);
    validationResponse.addError(insertErrorWithException);
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    Map<String, String> sfConnectorConfigWithErrors = new HashMap<>(sfConnectorConfig);
    sfConnectorConfigWithErrors.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    sfConnectorConfigWithErrors.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    InMemoryKafkaRecordErrorReporter kafkaRecordErrorReporter =
        new InMemoryKafkaRecordErrorReporter();
    BufferedTopicPartitionChannel topicPartitionChannel =
        new BufferedTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            new StreamingBufferThreshold(1000, 10_000_000, 10000),
            sfConnectorConfigWithErrors,
            kafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            schemaEvolutionService,
            new InsertErrorMapper());

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    BufferedTopicPartitionChannel.StreamingBuffer streamingBuffer =
        topicPartitionChannel.new StreamingBuffer();
    streamingBuffer.insert(records.get(0));

    assert topicPartitionChannel.insertRecords(streamingBuffer).hasErrors();

    assert kafkaRecordErrorReporter.getReportedRecords().size() == 1;
  }
}
