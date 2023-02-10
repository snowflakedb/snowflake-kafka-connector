package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.internal.TestUtils.createBigAvroRecords;
import static com.snowflake.kafka.connector.internal.TestUtils.createNativeJsonSinkRecords;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.ingestsdk.IngestSdkProvider;
import com.snowflake.kafka.connector.internal.ingestsdk.KcStreamingIngestClient;
import com.snowflake.kafka.connector.internal.ingestsdk.StreamingClientManager;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TopicPartitionChannelTest {
  @Mock private KafkaRecordErrorReporter mockKafkaRecordErrorReporter;
  @Mock private SnowflakeStreamingIngestChannel mockStreamingChannel;
  @Mock private SinkTaskContext mockSinkTaskContext;
  @Mock private KcStreamingIngestClient mockKcStreamingIngestClient;
  @Mock private StreamingClientManager mockStreamingClientManager;
  @Mock private SnowflakeConnectionService mockConn;
  @Mock private StreamingBufferThreshold mockStreamingBufferThreshold;

  // constants
  private static final String TOPIC = "TEST";
  private static final int PARTITION = 0;
  private static final String TEST_CHANNEL_NAME =
      SnowflakeSinkServiceV2.partitionChannelKey(TOPIC, PARTITION);
  private static final String TEST_TABLE_NAME = "TEST_TABLE";
  private static final long INVALID_OFFSET_VALUE = -1L;

  private final boolean enableSchematization;
  private final int taskId = 0;

  // models
  private TopicPartition topicPartition;
  private Map<String, String> sfConnectorConfig;
  private SFException SF_EXCEPTION = new SFException(ErrorCode.INVALID_CHANNEL, "INVALID_CHANNEL");

  // expected mock verification count
  private int expectedCallGetValidClientCount = 1;
  private int expectedCallOpenChannelCount = 1;
  private int expectedCallGetTaskIdCount = 1;

  public TopicPartitionChannelTest(boolean enableSchematization) {
    this.enableSchematization = enableSchematization;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  @Before
  public void setupEachTest() {
    // recreate mocks
    this.mockStreamingChannel = Mockito.mock(SnowflakeStreamingIngestChannel.class);
    this.mockKafkaRecordErrorReporter = Mockito.mock(KafkaRecordErrorReporter.class);
    this.mockSinkTaskContext = Mockito.mock(SinkTaskContext.class);
    this.mockKcStreamingIngestClient = Mockito.mock(KcStreamingIngestClient.class);
    this.mockStreamingClientManager = Mockito.mock(StreamingClientManager.class);
    this.mockConn = Mockito.mock(SnowflakeConnectionService.class);
    this.mockStreamingBufferThreshold = Mockito.mock(StreamingBufferThreshold.class);

    // sunny mock interactions and verifications
    Mockito.when(this.mockStreamingClientManager.getValidClient(this.taskId))
        .thenReturn(this.mockKcStreamingIngestClient);
    Mockito.when(
            this.mockKcStreamingIngestClient.openChannel(
                Mockito.refEq(TEST_CHANNEL_NAME),
                ArgumentMatchers.any(Map.class),
                Mockito.refEq(TEST_TABLE_NAME)))
        .thenReturn(this.mockStreamingChannel);
    Mockito.when(this.mockConn.getTaskId()).thenReturn(this.taskId);
    expectedCallGetValidClientCount = 1;
    expectedCallOpenChannelCount = 1;
    expectedCallGetTaskIdCount = 1;

    this.topicPartition = new TopicPartition(TOPIC, PARTITION);
    this.sfConnectorConfig = TestUtils.getConfig();
    this.sfConnectorConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
        Boolean.toString(this.enableSchematization));

    IngestSdkProvider.setStreamingClientManager(this.mockStreamingClientManager);
  }

  @After
  public void afterEachTest() {
    // need to reset client manager since it is global static variable
    IngestSdkProvider.setStreamingClientManager(new StreamingClientManager(new HashMap<>()));

    // verify the mocks setup above
    Mockito.verify(this.mockStreamingClientManager, Mockito.times(expectedCallGetValidClientCount))
        .getValidClient(this.taskId);
    Mockito.verify(this.mockKcStreamingIngestClient, Mockito.times(expectedCallOpenChannelCount))
        .openChannel(
            Mockito.refEq(TEST_CHANNEL_NAME),
            ArgumentMatchers.any(Map.class),
            Mockito.refEq(TEST_TABLE_NAME));
    Mockito.verify(this.mockConn, Mockito.times(expectedCallGetTaskIdCount)).getTaskId();
  }

  @Test
  public void testTopicPartitionChannelInit_streamingClientClosed() {
    Mockito.when(
            this.mockStreamingClientManager.getValidClient(ArgumentMatchers.any(Integer.class)))
        .thenThrow(SnowflakeErrors.ERROR_3009.getException());
    this.expectedCallOpenChannelCount = 0; // constructor fails before open

    TestUtils.assertError(
        SnowflakeErrors.ERROR_3009,
        () ->
            new TopicPartitionChannel(
                topicPartition,
                TEST_CHANNEL_NAME,
                TEST_TABLE_NAME,
                mockStreamingBufferThreshold,
                sfConnectorConfig,
                mockKafkaRecordErrorReporter,
                mockSinkTaskContext,
                mockConn));
  }

  @Test
  public void testFetchOffsetTokenWithRetry_null() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());

    Mockito.verify(this.mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
  }

  @Test
  public void testFetchOffsetTokenWithRetry_validLong() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn("100");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    Assert.assertEquals(100L, topicPartitionChannel.fetchOffsetTokenWithRetry());

    Mockito.verify(mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
  }

  @Test
  public void testFirstRecordForChannel() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(
            this.mockStreamingBufferThreshold.isFlushBufferedBytesBased(
                ArgumentMatchers.any(Long.class)))
        .thenReturn(true);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

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

    Mockito.verify(mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
    Mockito.verify(mockStreamingChannel, Mockito.times(1))
        .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
    Mockito.verify(this.mockStreamingBufferThreshold, Mockito.times(1))
        .isFlushBufferedBytesBased(ArgumentMatchers.any(Long.class));
  }

  @Test
  public void testCloseChannelException() throws Exception {
    CompletableFuture mockFuture = Mockito.mock(CompletableFuture.class);

    Mockito.when(mockStreamingChannel.close()).thenReturn(mockFuture);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);
    Mockito.when(mockFuture.get()).thenThrow(new InterruptedException("Interrupted Exception"));

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    topicPartitionChannel.closeChannel();

    Mockito.verify(mockStreamingChannel, Mockito.times(1)).close();
    Mockito.verify(mockStreamingChannel, Mockito.times(1)).getFullyQualifiedName();
    Mockito.verify(mockFuture, Mockito.times(1)).get();
  }

  @Test
  public void testFetchOffsetTokenWithRetry_SFException() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(SF_EXCEPTION);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    long fetchedOffset;
    try {
      this.expectedCallOpenChannelCount++; // retry getting offset reopens channel
      fetchedOffset = topicPartitionChannel.fetchOffsetTokenWithRetry();
    } catch (SFException ex) {
      fetchedOffset = INVALID_OFFSET_VALUE;
    }

    assert fetchedOffset == INVALID_OFFSET_VALUE;

    Mockito.verify(this.mockStreamingChannel, Mockito.times(MAX_GET_OFFSET_TOKEN_RETRIES + 1))
        .getLatestCommittedOffsetToken();
  }

  /* SFExceptions are retried and goes into fallback where it will reopen the channel and return a
  0 offsetToken */
  @Test
  public void testFetchOffsetTokenWithRetry_validOffsetTokenAfterMaxRetrySFExceptions() {
    final String offsetTokenAfterMaxAttempts = "0";

    // max retry is currently 3, so throw on first 3 and return correct on last retry
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION)
        .thenThrow(SF_EXCEPTION)
        .thenReturn(offsetTokenAfterMaxAttempts);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    Assert.assertEquals(
        Long.parseLong(offsetTokenAfterMaxAttempts),
        topicPartitionChannel.fetchOffsetTokenWithRetry());
    this.expectedCallOpenChannelCount++; // retry getting offset reopens channel

    Mockito.verify(this.mockStreamingChannel, Mockito.times(MAX_GET_OFFSET_TOKEN_RETRIES + 1))
        .getLatestCommittedOffsetToken();
  }

  /* No retries are since it throws NumberFormatException */
  @Test
  public void testFetchOffsetTokenWithRetry_InvalidNumber() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn("invalidNo");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    long fetchedOffset;
    try {
      fetchedOffset = topicPartitionChannel.fetchOffsetTokenWithRetry();
    } catch (ConnectException exception) {
      fetchedOffset = INVALID_OFFSET_VALUE;
      Assert.assertTrue(exception.getMessage().contains("invalidNo"));
    }

    assert fetchedOffset == INVALID_OFFSET_VALUE;

    Mockito.verify(this.mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
  }

  /* No retries and fallback here too since it throws an unknown NPE. */
  @Test
  public void testFetchOffsetTokenWithRetry_NullPointerException() {
    NullPointerException npe = new NullPointerException("NPE");
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(npe);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    long fetchedOffset;
    try {
      fetchedOffset = topicPartitionChannel.fetchOffsetTokenWithRetry();
    } catch (NullPointerException ex) {
      fetchedOffset = INVALID_OFFSET_VALUE;
      assert ex.getMessage().equals(npe.getMessage());
    }

    assert fetchedOffset == INVALID_OFFSET_VALUE;

    Mockito.verify(this.mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
  }

  /* No retries and fallback here too since it throws an unknown runtime exception. */
  @Test(expected = RuntimeException.class)
  public void testFetchOffsetTokenWithRetry_RuntimeException() {
    RuntimeException runtimeException = new RuntimeException("runtime exception");
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(runtimeException);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    try {
      Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
    } catch (RuntimeException ex) {
    }

    long fetchedOffset;
    try {
      fetchedOffset = topicPartitionChannel.fetchOffsetTokenWithRetry();
    } catch (NullPointerException ex) {
      fetchedOffset = INVALID_OFFSET_VALUE;
      assert ex.getMessage().equals(runtimeException.getMessage());
    }

    assert fetchedOffset == INVALID_OFFSET_VALUE;

    Mockito.verify(this.mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
  }

  /*
  try insert rows twice, first will fail, second reopens channel and succeeds
  first insertrows:
     1. precompute offset because new channel - got null offset
     2. try insert, fail - throw sfexception
     3. reopen channel
     4. get offset token - null offset
  second insert rows:
     1. try insert, succeed
  */
  @Test
  public void testInsertRows_SuccessAfterReopenChannel() throws Exception {
    final int noOfRecords = 5;
    int expectedCallInsertRowCount = 0;
    int expectedCallGetOffsetCount = 0;

    // first insert fails, so first offset response is null
    // second insert succeeds, and offset is bumped accordingly
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION)
        .thenReturn(new InsertValidationResponse());
    // get offset token is called twice - after channel re-open and before a new partition is just
    // created (In Precomputation). So first two returns are the failure, second two are the success
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenReturn(null)
        .thenReturn(null)
        .thenReturn(Long.toString(noOfRecords - 1))
        .thenReturn(Long.toString(noOfRecords - 1));
    Mockito.when(
            mockStreamingBufferThreshold.isFlushBufferedBytesBased(
                ArgumentMatchers.any(Long.class)))
        .thenReturn(true);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    // verify channel did nothing
    Mockito.verify(this.mockStreamingChannel, Mockito.times(expectedCallInsertRowCount))
        .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
    Mockito.verify(this.mockStreamingChannel, Mockito.times(expectedCallGetOffsetCount))
        .getLatestCommittedOffsetToken();

    // TEST inserting - should fail
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, TOPIC, PARTITION);
    records.forEach(topicPartitionChannel::insertRecordToBuffer);
    this.expectedCallOpenChannelCount++; // should reopen channel here

    expectedCallInsertRowCount += 1;
    expectedCallGetOffsetCount += 2;
    Mockito.verify(this.mockStreamingChannel, Mockito.times(expectedCallInsertRowCount))
        .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
    Mockito.verify(this.mockStreamingChannel, Mockito.times(expectedCallGetOffsetCount))
        .getLatestCommittedOffsetToken();

    // TEST inserting - should succeed
    records.forEach(topicPartitionChannel::insertRecordToBuffer);
    expectedCallInsertRowCount += noOfRecords;
    Mockito.verify(this.mockStreamingChannel, Mockito.times(expectedCallInsertRowCount))
        .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
    Mockito.verify(this.mockStreamingChannel, Mockito.times(expectedCallGetOffsetCount))
        .getLatestCommittedOffsetToken();

    // expected number of records were ingested
    Assert.assertEquals(noOfRecords - 1, topicPartitionChannel.fetchOffsetTokenWithRetry());
    expectedCallGetOffsetCount += 1; // one more get offset call

    Mockito.verify(this.mockStreamingChannel, Mockito.times(expectedCallGetOffsetCount))
        .getLatestCommittedOffsetToken();
    Mockito.verify(this.mockStreamingBufferThreshold, Mockito.times(6))
        .isFlushBufferedBytesBased(ArgumentMatchers.any(Long.class));
  }

  @Test
  public void testInsertRowsWithSchemaEvolution() throws Exception {
    if (this.sfConnectorConfig
        .get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG)
        .equals("true")) {
      int noOfRecords = 0;

      // this response should insert the row
      InsertValidationResponse validResponse = new InsertValidationResponse();
      SinkRecord validRecord = TestUtils.createNativeJsonSinkRecord(noOfRecords, TOPIC, PARTITION);
      noOfRecords++;

      // this response should get row sent to dlq
      InsertValidationResponse failureResponse = new InsertValidationResponse();
      InsertValidationResponse.InsertError insertError1 =
          new InsertValidationResponse.InsertError("CONTENT", noOfRecords);
      insertError1.setException(SF_EXCEPTION);
      failureResponse.addError(insertError1);
      SinkRecord failureRecord =
          TestUtils.createNativeJsonSinkRecord(noOfRecords, TOPIC, PARTITION);
      noOfRecords++;

      // this response should make schema evolve
      InsertValidationResponse evolveSchemaResponse = new InsertValidationResponse();
      InsertValidationResponse.InsertError insertError2 =
          new InsertValidationResponse.InsertError("CONTENT", noOfRecords);
      insertError2.setException(SF_EXCEPTION);
      insertError2.setExtraColNames(Collections.singletonList("gender")); // will evolve schema
      evolveSchemaResponse.addError(insertError2);
      SinkRecord evolveSchemaRecord =
          TestUtils.createNativeJsonSinkRecord(noOfRecords, TOPIC, PARTITION);
      noOfRecords++;

      this.sfConnectorConfig.put(
          ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
      this.sfConnectorConfig.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");

      // mocks
      Mockito.when(
              this.mockStreamingChannel.insertRow(
                  ArgumentMatchers.any(), ArgumentMatchers.any(String.class)))
          .thenReturn(validResponse)
          .thenReturn(failureResponse)
          .thenReturn(evolveSchemaResponse);
      Mockito.when(
              this.mockConn.hasSchemaEvolutionPermission(
                  ArgumentMatchers.any(), ArgumentMatchers.any()))
          .thenReturn(true);
      Mockito.doNothing()
          .when(this.mockConn)
          .appendColumnsToTable(ArgumentMatchers.any(), ArgumentMatchers.any());
      Mockito.when(mockStreamingBufferThreshold.isFlushTimeBased(ArgumentMatchers.any(Long.class)))
          .thenReturn(true);

      // test
      TopicPartitionChannel topicPartitionChannel =
          new TopicPartitionChannel(
              topicPartition,
              TEST_CHANNEL_NAME,
              TEST_TABLE_NAME,
              mockStreamingBufferThreshold,
              this.sfConnectorConfig,
              this.mockKafkaRecordErrorReporter,
              this.mockSinkTaskContext,
              this.mockConn);

      topicPartitionChannel.insertRecordToBuffer(validRecord);
      topicPartitionChannel.insertRecordToBuffer(failureRecord);
      topicPartitionChannel.insertRecordToBuffer(evolveSchemaRecord);

      topicPartitionChannel.insertBufferedRecordsIfFlushTimeThresholdReached();
      expectedCallOpenChannelCount++;

      // Verify that the buffer is cleaned up and one record is in the DLQ (one error reported)
      Assert.assertTrue(topicPartitionChannel.isPartitionBufferEmpty());
      Mockito.verify(this.mockKafkaRecordErrorReporter, Mockito.times(1))
          .reportError(Mockito.refEq(failureRecord), ArgumentMatchers.any(SFException.class));

      Mockito.verify(this.mockStreamingChannel, Mockito.times(noOfRecords))
          .insertRow(ArgumentMatchers.any(), ArgumentMatchers.any(String.class));
      Mockito.verify(this.mockConn, Mockito.times(1))
          .hasSchemaEvolutionPermission(ArgumentMatchers.any(), ArgumentMatchers.any());
      Mockito.verify(this.mockConn, Mockito.times(1))
          .appendColumnsToTable(ArgumentMatchers.any(), ArgumentMatchers.any());
      Mockito.verify(mockStreamingBufferThreshold, Mockito.times(1))
          .isFlushTimeBased(ArgumentMatchers.any(Long.class));
    } else {
      // not streaming means nothing is executed
      this.expectedCallGetValidClientCount = 0;
      this.expectedCallOpenChannelCount = 0;
      this.expectedCallGetTaskIdCount = 0;
    }
  }

  /* SFExceptions is thrown in first attempt of insert rows. It is also thrown while refetching
  committed offset from snowflake after reopening the channel */
  @Test(expected = SFException.class)
  public void testInsertRows_GetOffsetTokenFailureAfterReopenChannel() throws Exception {
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION);

    // Send exception in fallback (i.e after reopen channel)
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(SF_EXCEPTION);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    try {
      this.expectedCallOpenChannelCount++; // retry getting offset reopens channel
      TopicPartitionChannel.StreamingBuffer streamingBuffer =
          topicPartitionChannel.new StreamingBuffer();
      streamingBuffer.insert(records.get(0));
      topicPartitionChannel.insertBufferedRecords(streamingBuffer);
    } catch (SFException ex) {
      Mockito.verify(this.mockStreamingChannel, Mockito.times(1))
          .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
      // get offset token is called once after channel re-open
      Mockito.verify(this.mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* Runtime exception does not perform any fallbacks. */
  @Test(expected = RuntimeException.class)
  public void testInsertRows_RuntimeException() throws Exception {
    RuntimeException exception = new RuntimeException("runtime exception");
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenThrow(exception);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    topicPartitionChannel.insertRecordToBuffer(records.get(0));

    try {
      topicPartitionChannel.insertBufferedRecords(topicPartitionChannel.getStreamingBuffer());
    } catch (RuntimeException ex) {
      Mockito.verify(this.mockStreamingChannel, Mockito.times(1))
          .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
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
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    topicPartitionChannel.insertRecordToBuffer(records.get(0));

    try {
      topicPartitionChannel.insertBufferedRecords(topicPartitionChannel.getStreamingBuffer());
    } catch (DataException ex) {
      Mockito.verify(this.mockStreamingChannel, Mockito.times(1))
          .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
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
            this.mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    this.sfConnectorConfig.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    this.sfConnectorConfig.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.mockStreamingBufferThreshold,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockConn);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    TopicPartitionChannel.StreamingBuffer streamingBuffer =
        topicPartitionChannel.new StreamingBuffer();
    streamingBuffer.insert(records.get(0));

    assert topicPartitionChannel.insertBufferedRecords(streamingBuffer).hasErrors();
    Mockito.verify(this.mockKafkaRecordErrorReporter, Mockito.times(1))
        .reportError(Mockito.refEq(records.get(0)), ArgumentMatchers.any(SFException.class));
    Mockito.verify(this.mockStreamingChannel, Mockito.times(1))
        .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
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
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);

    this.sfConnectorConfig.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    this.sfConnectorConfig.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
    this.sfConnectorConfig.put(ERRORS_LOG_ENABLE_CONFIG, "true");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.mockStreamingBufferThreshold,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockConn);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    TopicPartitionChannel.StreamingBuffer streamingBuffer =
        topicPartitionChannel.new StreamingBuffer();
    streamingBuffer.insert(records.get(0));

    assert topicPartitionChannel.insertBufferedRecords(streamingBuffer).hasErrors();
    Mockito.verify(this.mockKafkaRecordErrorReporter, Mockito.times(1))
        .reportError(Mockito.refEq(records.get(0)), ArgumentMatchers.any(SFException.class));
    Mockito.verify(mockStreamingChannel, Mockito.times(1))
        .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
  }

  // --------------- TEST THRESHOLDS ---------------
  // insert 5 records, 4th will trigger the byte threshold, 5th will trigger time threshold
  @Test
  public void testBufferBytesThreshold() throws Exception {
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(
            mockStreamingBufferThreshold.isFlushBufferedBytesBased(
                ArgumentMatchers.any(Long.class)))
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(true)
        .thenReturn(false);
    Mockito.when(mockStreamingBufferThreshold.isFlushTimeBased(ArgumentMatchers.any(Long.class)))
        .thenReturn(true);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    List<SinkRecord> records = createNativeJsonSinkRecords(0, 5, "test", 0);
    // insert 3 records, verify rows in buffer
    topicPartitionChannel.insertRecordToBuffer(records.get(0));
    topicPartitionChannel.insertRecordToBuffer(records.get(1));
    topicPartitionChannel.insertRecordToBuffer(records.get(2));
    Assert.assertTrue(!topicPartitionChannel.isPartitionBufferEmpty());
    Mockito.verify(mockStreamingChannel, Mockito.times(0))
        .insertRows(ArgumentMatchers.any(), ArgumentMatchers.any());

    // insert 4th record, verify byte flush - no rows in buffer
    topicPartitionChannel.insertRecordToBuffer(records.get(3));
    Assert.assertTrue(topicPartitionChannel.isPartitionBufferEmpty());
    Mockito.verify(mockStreamingChannel, Mockito.times(1))
        .insertRows(ArgumentMatchers.any(), ArgumentMatchers.any());

    // insert 5th record
    topicPartitionChannel.insertRecordToBuffer(records.get(4));

    // flush on time buffer, verify time flush - no rows in buffer
    topicPartitionChannel.insertBufferedRecordsIfFlushTimeThresholdReached();
    Assert.assertTrue(topicPartitionChannel.isPartitionBufferEmpty());
    Mockito.verify(mockStreamingChannel, Mockito.times(2))
        .insertRows(ArgumentMatchers.any(), ArgumentMatchers.any());

    Mockito.verify(mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
    Mockito.verify(mockStreamingBufferThreshold, Mockito.times(1))
        .isFlushTimeBased(ArgumentMatchers.any(Long.class));
    Mockito.verify(mockStreamingBufferThreshold, Mockito.times(5))
        .isFlushBufferedBytesBased(ArgumentMatchers.any(Long.class));
  }

  // insert 3 records, 2nd will trigger the byte threshold, 3rd will trigger time threshold
  @Test
  public void testBigAvroBufferBytesThreshold() throws Exception {
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(
            mockStreamingBufferThreshold.isFlushBufferedBytesBased(
                ArgumentMatchers.any(Long.class)))
        .thenReturn(false)
        .thenReturn(true)
        .thenReturn(false);
    Mockito.when(mockStreamingBufferThreshold.isFlushTimeBased(ArgumentMatchers.any(Long.class)))
        .thenReturn(true);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            mockStreamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockConn);

    // Sending 3 records will trigger a buffer bytes based threshold after 2 records have been
    // added. Size of each record after serialization to Json is ~6 KBytes
    List<SinkRecord> records = createBigAvroRecords(0, 3, "test", 0);
    // insert 1 record, verify row in buffer
    topicPartitionChannel.insertRecordToBuffer(records.get(0));
    Assert.assertTrue(!topicPartitionChannel.isPartitionBufferEmpty());
    Mockito.verify(mockStreamingChannel, Mockito.times(0))
        .insertRows(ArgumentMatchers.any(), ArgumentMatchers.any());

    // insert 2nd record, verify byte flush - no rows in buffer
    topicPartitionChannel.insertRecordToBuffer(records.get(1));
    Assert.assertTrue(topicPartitionChannel.isPartitionBufferEmpty());
    Mockito.verify(mockStreamingChannel, Mockito.times(1))
        .insertRows(ArgumentMatchers.any(), ArgumentMatchers.any());

    // insert 3th record
    topicPartitionChannel.insertRecordToBuffer(records.get(2));

    // flush on time buffer, verify time flush - no rows in buffer
    topicPartitionChannel.insertBufferedRecordsIfFlushTimeThresholdReached();
    Assert.assertTrue(topicPartitionChannel.isPartitionBufferEmpty());

    Mockito.verify(mockStreamingChannel, Mockito.times(1)).getLatestCommittedOffsetToken();
    Mockito.verify(mockStreamingBufferThreshold, Mockito.times(1))
        .isFlushTimeBased(ArgumentMatchers.any(Long.class));
    Mockito.verify(mockStreamingBufferThreshold, Mockito.times(3))
        .isFlushBufferedBytesBased(ArgumentMatchers.any(Long.class));
  }
}
