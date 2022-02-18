package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicPartitionChannelTest {

  @Mock private KafkaRecordErrorReporter mockKafkaRecordErrorReporter;

  @Mock private SnowflakeStreamingIngestClient mockStreamingClient;

  @Mock private SnowflakeStreamingIngestChannel mockStreamingChannel;

  @Mock private SinkTaskContext mockSinkTaskContext;

  private static final String TOPIC = "TEST";

  private static final int PARTITION = 0;

  private static final String TEST_CHANNEL_NAME =
      SnowflakeSinkServiceV2.partitionChannelKey(TOPIC, PARTITION);
  private static final String TEST_TABLE_NAME = "TEST_TABLE";

  private TopicPartition topicPartition;

  private Map<String, String> sfConnectorConfig;

  private StreamingBufferThreshold streamingBufferThreshold;

  private SFException SF_EXCEPTION = new SFException(ErrorCode.INVALID_CHANNEL, "INVALID_CHANNEL");

  @Before
  public void setupEachTest() {
    Mockito.when(mockStreamingClient.isClosed()).thenReturn(false);
    Mockito.when(mockStreamingClient.openChannel(ArgumentMatchers.any(OpenChannelRequest.class)))
        .thenReturn(mockStreamingChannel);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);
    this.topicPartition = new TopicPartition(TOPIC, PARTITION);
    this.sfConnectorConfig = TestUtils.getConfig();
    this.streamingBufferThreshold = new StreamingBufferThreshold(10, 10_000, 1);
  }

  @Test(expected = IllegalStateException.class)
  public void testTopicPartitionChannelInit_streamingClientClosed() {
    Mockito.when(mockStreamingClient.isClosed()).thenReturn(true);
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);
  }

  @Test
  public void testFetchOffsetTokenWithRetry_null() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

  @Test
  public void testFetchOffsetTokenWithRetry_validLong() {

    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn("100");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    Assert.assertEquals(100L, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

  // TODO:: Fix this test
  @Test
  public void testFirstRecordForChannel() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    JsonConverter converter = new JsonConverter();
    HashMap<String, String> converterConfig = new HashMap<String, String>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, true);
    SchemaAndValue input =
        converter.toConnectData("test", "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;

    SinkRecord record1 =
        new SinkRecord(
            "test",
            0,
            Schema.STRING_SCHEMA,
            "test_key" + offset,
            input.schema(),
            input.value(),
            offset);

    topicPartitionChannel.insertRecordToBuffer(record1);

    Assert.assertEquals(-1l, topicPartitionChannel.getOffsetPersistedInSnowflake());

    Assert.assertTrue(topicPartitionChannel.isPartitionBufferEmpty());
  }

  @Test
  public void testCloseChannelException() throws Exception {
    CompletableFuture mockFuture = Mockito.mock(CompletableFuture.class);

    Mockito.when(mockStreamingChannel.close()).thenReturn(mockFuture);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);

    Mockito.when(mockFuture.get()).thenThrow(new InterruptedException("Interrupted Exception"));
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    topicPartitionChannel.closeChannel();
  }

  /* Only SFExceptions are retried and goes into fallback. */
  @Test(expected = SFException.class)
  public void testFetchOffsetTokenWithRetry_SFException() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    try {
      Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
    } catch (SFException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(2)).openChannel(ArgumentMatchers.any());
      Mockito.verify(
              topicPartitionChannel.getChannel(), Mockito.times(MAX_GET_OFFSET_TOKEN_RETRIES + 1))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* SFExceptions are retried and goes into fallback where it will reopen the channel and return a 0 offsetToken */
  @Test
  public void testFetchOffsetTokenWithRetry_validOffsetTokenAfterThreeSFExceptions() {
    final String offsetTokenAfterMaxAttempts = "0";

    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION)
        .thenReturn(offsetTokenAfterMaxAttempts);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    Assert.assertEquals(
        Long.parseLong(offsetTokenAfterMaxAttempts),
        topicPartitionChannel.fetchOffsetTokenWithRetry());
    Mockito.verify(mockStreamingClient, Mockito.times(2)).openChannel(ArgumentMatchers.any());
    Mockito.verify(
            topicPartitionChannel.getChannel(), Mockito.times(MAX_GET_OFFSET_TOKEN_RETRIES + 1))
        .getLatestCommittedOffsetToken();
  }

  /* No retries are since it throws NumberFormatException */
  @Test(expected = ConnectException.class)
  public void testFetchOffsetTokenWithRetry_InvalidNumber() {

    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn("invalidNo");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    try {
      topicPartitionChannel.fetchOffsetTokenWithRetry();
      Assert.fail("Should throw exception");
    } catch (ConnectException exception) {
      // Open channel is not called again.
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());

      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .getLatestCommittedOffsetToken();
      Assert.assertTrue(exception.getMessage().contains("invalidNo"));
      throw exception;
    }
  }

  /* No reteries and fallback here too since it throws an unknown NPE. */
  @Test(expected = NullPointerException.class)
  public void testFetchOffsetTokenWithRetry_NullPointerException() {
    NullPointerException exception = new NullPointerException("NPE");
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(exception);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    try {
      Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
    } catch (NullPointerException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* No reteries and fallback here too since it throws an unknown NPE. */
  @Test(expected = RuntimeException.class)
  public void testFetchOffsetTokenWithRetry_RuntimeException() {
    RuntimeException exception = new RuntimeException("runtime exception");
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(exception);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    try {
      Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
    } catch (RuntimeException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* Only SFExceptions are retried and goes into fallback. */
  @Test(expected = RetriableException.class)
  public void testInsertRows_SFException() throws Exception {
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION);

    // get null from snowflake first time it is called and null for second time too since insert
    // rows was failure
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenReturn(null)
        .thenReturn(null);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    try {
      TopicPartitionChannel.StreamingBuffer streamingBuffer =
          topicPartitionChannel.new StreamingBuffer();
      streamingBuffer.insert(records.get(0));
      topicPartitionChannel.insertBufferedRows(streamingBuffer);
    } catch (RetriableException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(2)).openChannel(ArgumentMatchers.any());
      // insert rows is only called once.
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
      // get offset token is called once for first record and once after channel re-open
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(2))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* SFExceptions is thrown in first attempt of insert rows. It is also thrown while refetching committed offset from snowflake after reopening the channel */
  @Test(expected = SFException.class)
  public void testInsertRows_GetOffsetTokenFailureAfterReopenChannel() throws Exception {
    InsertValidationResponse validationResponse = new InsertValidationResponse();
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION);

    // get null from snowflake first time it is called and null for second time too since insert
    // rows was failure
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenReturn(null)
        .thenThrow(SF_EXCEPTION);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    try {
      TopicPartitionChannel.StreamingBuffer streamingBuffer =
          topicPartitionChannel.new StreamingBuffer();
      streamingBuffer.insert(records.get(0));
      InsertValidationResponse response = topicPartitionChannel.insertBufferedRows(streamingBuffer);
    } catch (SFException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(2)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
      // get offset token is called once for first record and once after channel re-open
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(2))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* Runtime exception doesnt perform any fallbacks. */
  @Test(expected = RuntimeException.class)
  public void testInsertRows_RuntimeException() throws Exception {
    RuntimeException exception = new RuntimeException("runtime exception");
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenThrow(exception);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    topicPartitionChannel.insertRecordToBuffer(records.get(0));

    try {
      topicPartitionChannel.insertBufferedRows(topicPartitionChannel.getStreamingBuffer());
    } catch (RuntimeException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
      throw ex;
    }
  }

  /* Valid response but has errors. */
  @Test(expected = DataException.class)
  public void testInsertRows_ValidationResponseHasErrors_NoErrorTolerance() throws Exception {
    InsertValidationResponse validationResponse = new InsertValidationResponse();
    validationResponse.addError(
        new InsertValidationResponse.InsertError("CONTENT", SF_EXCEPTION, 0));
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    topicPartitionChannel.insertRecordToBuffer(records.get(0));

    try {
      topicPartitionChannel.insertBufferedRows(topicPartitionChannel.getStreamingBuffer());
    } catch (DataException ex) {
      throw ex;
    }
  }

  /* Valid response but has errors, error tolerance is ALL. Meaning it will ignore the error.  */
  @Test
  public void testInsertRows_ValidationResponseHasErrors_ErrorTolerance_ALL() throws Exception {
    InsertValidationResponse validationResponse = new InsertValidationResponse();
    validationResponse.addError(
        new InsertValidationResponse.InsertError("CONTENT", SF_EXCEPTION, 0));
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    Map<String, String> sfConnectorConfigWithErrors = new HashMap<>(sfConnectorConfig);
    sfConnectorConfigWithErrors.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    sfConnectorConfigWithErrors.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    InMemoryKafkaRecordErrorReporter kafkaRecordErrorReporter =
        new InMemoryKafkaRecordErrorReporter();
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            new StreamingBufferThreshold(1000, 10_000_000, 10000),
            sfConnectorConfigWithErrors,
            kafkaRecordErrorReporter,
            mockSinkTaskContext);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    TopicPartitionChannel.StreamingBuffer streamingBuffer =
        topicPartitionChannel.new StreamingBuffer();
    streamingBuffer.insert(records.get(0));

    assert topicPartitionChannel.insertBufferedRows(streamingBuffer).hasErrors();

    assert kafkaRecordErrorReporter.getReportedRecords().size() == 1;
  }

  /* Valid response but has errors, error tolerance is ALL. Meaning it will ignore the error.  */
  @Test
  public void testInsertRows_ValidationResponseHasErrors_ErrorTolerance_ALL_LogEnableTrue()
      throws Exception {
    InsertValidationResponse validationResponse = new InsertValidationResponse();
    validationResponse.addError(
        new InsertValidationResponse.InsertError("CONTENT", SF_EXCEPTION, 0));
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    Map<String, String> sfConnectorConfigWithErrors = new HashMap<>(sfConnectorConfig);
    sfConnectorConfigWithErrors.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    sfConnectorConfigWithErrors.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    sfConnectorConfigWithErrors.put(ERRORS_LOG_ENABLE_CONFIG, "true");

    InMemoryKafkaRecordErrorReporter kafkaRecordErrorReporter =
        new InMemoryKafkaRecordErrorReporter();
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfigWithErrors,
            kafkaRecordErrorReporter,
            mockSinkTaskContext);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    TopicPartitionChannel.StreamingBuffer streamingBuffer =
        topicPartitionChannel.new StreamingBuffer();
    streamingBuffer.insert(records.get(0));

    assert topicPartitionChannel.insertBufferedRows(streamingBuffer).hasErrors();

    assert kafkaRecordErrorReporter.getReportedRecords().size() == 1;
  }
}
