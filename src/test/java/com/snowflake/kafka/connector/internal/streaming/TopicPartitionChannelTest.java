package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.TestUtils.createBigAvroRecords;
import static com.snowflake.kafka.connector.internal.TestUtils.createNativeJsonSinkRecords;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;
import static org.mockito.ArgumentMatchers.eq;

import com.codahale.metrics.MetricRegistry;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
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

  @Mock private SnowflakeStreamingIngestClient mockStreamingClient;

  @Mock private SnowflakeStreamingIngestChannel mockStreamingChannel;

  @Mock private SinkTaskContext mockSinkTaskContext;

  @Mock private SnowflakeConnectionService mockSnowflakeConnectionService;

  @Mock private SnowflakeTelemetryService mockTelemetryService;

  private static final String TOPIC = "TEST";

  private static final int PARTITION = 0;

  private static final String TEST_CHANNEL_NAME =
      SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, TOPIC, PARTITION);
  private static final String TEST_TABLE_NAME = "TEST_TABLE";

  private TopicPartition topicPartition;

  private Map<String, String> sfConnectorConfig;

  private BufferThreshold streamingBufferThreshold;

  private SFException SF_EXCEPTION = new SFException(ErrorCode.INVALID_CHANNEL, "INVALID_CHANNEL");

  private final boolean enableSchematization;

  public TopicPartitionChannelTest(boolean enableSchematization) {
    this.enableSchematization = enableSchematization;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
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

  @Test(expected = IllegalStateException.class)
  public void testTopicPartitionChannelInit_streamingClientClosed() {
    Mockito.when(mockStreamingClient.isClosed()).thenReturn(true);
    new TopicPartitionChannel(
        mockStreamingClient,
        topicPartition,
        TEST_CHANNEL_NAME,
        TEST_TABLE_NAME,
        streamingBufferThreshold,
        sfConnectorConfig,
        mockKafkaRecordErrorReporter,
        mockSinkTaskContext,
        mockTelemetryService);
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
            mockSinkTaskContext,
            mockTelemetryService);

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
            mockSinkTaskContext,
            mockTelemetryService);

    Assert.assertEquals(100L, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

  // TODO:: Fix this test
  @Test
  public void testFirstRecordForChannel() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
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
            mockSinkTaskContext,
            mockTelemetryService);

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

    topicPartitionChannel.insertRecord(record1);

    Assert.assertEquals(-1l, topicPartitionChannel.getOffsetPersistedInSnowflake());
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
            true,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            new RecordService(mockTelemetryService),
            mockTelemetryService,
            false,
            null);

    topicPartitionChannel.closeChannel();
  }

  /* Only SFExceptions are retried and goes into fallback. */
  @Test(expected = SFException.class)
  public void testFetchOffsetTokenWithRetry_SFException() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(SF_EXCEPTION);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockTelemetryService);

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
            mockSinkTaskContext,
            mockTelemetryService);

    int expectedRetries = MAX_GET_OFFSET_TOKEN_RETRIES;
    Mockito.verify(mockStreamingClient, Mockito.times(2)).openChannel(ArgumentMatchers.any());
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(++expectedRetries))
        .getLatestCommittedOffsetToken();

    Assert.assertEquals(
        Long.parseLong(offsetTokenAfterMaxAttempts),
        topicPartitionChannel.fetchOffsetTokenWithRetry());
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(++expectedRetries))
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
            mockSinkTaskContext,
            mockTelemetryService);

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

  /* No retries and fallback here too since it throws an unknown NPE. */
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
            mockSinkTaskContext,
            mockTelemetryService);

    try {
      Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
    } catch (NullPointerException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* No retries and fallback here too since it throws an unknown NPE. */
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
            mockSinkTaskContext,
            mockTelemetryService);

    try {
      Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
    } catch (RuntimeException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }

  /* Only SFExceptions goes into fallback -> reopens channel, fetch offsetToken and throws Appropriate exception */
  @Test
  public void testInsertRows_SuccessAfterReopenChannel() throws Exception {
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION);

    // get null from snowflake first time it is called and null for second time too since insert
    // rows was failure
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
            mockSinkTaskContext,
            mockTelemetryService);
    final int noOfRecords = 5;
    // Since record 0 was not able to ingest, all records in this batch will not be added into the
    // buffer.
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, TOPIC, PARTITION);

    records.forEach(topicPartitionChannel::insertRecord);

    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(noOfRecords))
        .insertRow(ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class));
    Mockito.verify(mockStreamingClient, Mockito.times(noOfRecords + 1))
        .openChannel(ArgumentMatchers.any());
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(noOfRecords + 1))
        .getLatestCommittedOffsetToken();

    // Now, it should be successful
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());

    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenReturn(Long.toString(noOfRecords - 1));

    // Retry the insert again, now everything should be ingested and the offset token should be
    // noOfRecords-1
    records.forEach(topicPartitionChannel::insertRecord);
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(noOfRecords * 2))
        .insertRow(ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class));

    Assert.assertEquals(noOfRecords - 1, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

  @Test
  public void testInsertRowsWithSchemaEvolution() throws Exception {
    if (this.sfConnectorConfig
        .get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG)
        .equals("true")) {
      InsertValidationResponse validationResponse1 = new InsertValidationResponse();
      InsertValidationResponse.InsertError insertError1 =
          new InsertValidationResponse.InsertError("CONTENT", 0);
      insertError1.setException(SF_EXCEPTION);
      validationResponse1.addError(insertError1);

      InsertValidationResponse validationResponse2 = new InsertValidationResponse();
      InsertValidationResponse.InsertError insertError2 =
          new InsertValidationResponse.InsertError("CONTENT", 0);
      insertError2.setException(SF_EXCEPTION);
      insertError2.setExtraColNames(Collections.singletonList("gender"));
      validationResponse2.addError(insertError2);

      Mockito.when(
              mockStreamingChannel.insertRow(
                  ArgumentMatchers.any(), ArgumentMatchers.any(String.class)))
          .thenReturn(new InsertValidationResponse())
          .thenReturn(validationResponse1)
          .thenReturn(validationResponse2);

      Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

      SnowflakeConnectionService conn = Mockito.mock(SnowflakeConnectionService.class);
      Mockito.when(
              conn.hasSchemaEvolutionPermission(ArgumentMatchers.any(), ArgumentMatchers.any()))
          .thenReturn(true);
      Mockito.doNothing()
          .when(conn)
          .appendColumnsToTable(ArgumentMatchers.any(), ArgumentMatchers.any());

      long bufferFlushTimeSeconds = 5L;
      StreamingBufferThreshold bufferThreshold =
          new StreamingBufferThreshold(bufferFlushTimeSeconds, 1_000 /* < 1KB */, 10000000L);

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
              this.enableSchematization,
              bufferThreshold,
              sfConnectorConfigWithErrors,
              kafkaRecordErrorReporter,
              mockSinkTaskContext,
              conn,
              new RecordService(),
              mockTelemetryService,
              false,
              null);

      final int noOfRecords = 3;
      List<SinkRecord> records =
          TestUtils.createNativeJsonSinkRecords(0, noOfRecords, TOPIC, PARTITION);

      records.forEach(topicPartitionChannel::insertRecord);

      // Verify one record is in the DLQ
      Assert.assertEquals(1, kafkaRecordErrorReporter.getReportedRecords().size());
    }
  }

  /* SFExceptions is thrown in first attempt of insert rows. It is also thrown while refetching committed offset from snowflake after reopening the channel */
  @Test(expected = SFException.class)
  public void testInsertRows_GetOffsetTokenFailureAfterReopenChannel() throws Exception {
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION);

    // Send exception in fallback (i.e after reopen channel)
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(SF_EXCEPTION);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            streamingBufferThreshold,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockTelemetryService);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    try {
      topicPartitionChannel.insertRecord(records.get(0));
    } catch (SFException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(2)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .insertRows(ArgumentMatchers.any(Iterable.class), ArgumentMatchers.any(String.class));
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
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
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
            mockSinkTaskContext,
            mockTelemetryService);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    try {
      topicPartitionChannel.insertRecord(records.get(0));
    } catch (RuntimeException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .insertRow(ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class));
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
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse);
    Mockito.doNothing()
        .when(mockTelemetryService)
        .reportKafkaConnectFatalError(ArgumentMatchers.anyString());

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
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
            new RecordService(mockTelemetryService),
            mockTelemetryService,
            false,
            null);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    try {
      topicPartitionChannel.insertRecord(records.get(0));
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
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
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
            mockSinkTaskContext,
            mockTelemetryService);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    assert topicPartitionChannel.insertRecord(records.get(0)).hasErrors();

    assert kafkaRecordErrorReporter.getReportedRecords().size() == 1;
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
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
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
            mockSinkTaskContext,
            mockTelemetryService);

    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, TOPIC, PARTITION);

    assert topicPartitionChannel.insertRecord(records.get(0)).hasErrors();

    assert kafkaRecordErrorReporter.getReportedRecords().size() == 1;
  }

  @Test
  public void testTopicPartitionChannelMetrics() throws Exception {
    // variables
    int noOfRecords = 5;

    // setup jmxreporter
    MetricRegistry metricRegistry = Mockito.spy(MetricRegistry.class);
    MetricsJmxReporter metricsJmxReporter =
        Mockito.spy(new MetricsJmxReporter(metricRegistry, TEST_CONNECTOR_NAME));

    // setup insert
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(mockStreamingChannel.close()).thenReturn(Mockito.mock(CompletableFuture.class));

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            this.mockStreamingClient,
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.enableSchematization,
            this.streamingBufferThreshold,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockSnowflakeConnectionService,
            new RecordService(),
            this.mockTelemetryService,
            true,
            metricsJmxReporter);

    // insert records
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, TOPIC, PARTITION);
    records.forEach(topicPartitionChannel::insertRecord);

    // verify metrics
    SnowflakeTelemetryChannelStatus resultStatus =
        topicPartitionChannel.getSnowflakeTelemetryChannelStatus();

    assert resultStatus.getOffsetPersistedInSnowflake()
        == topicPartitionChannel.getOffsetPersistedInSnowflake();
    assert resultStatus.getOffsetPersistedInSnowflake() == -1;
    assert resultStatus.getProcessedOffset() == topicPartitionChannel.getProcessedOffset();
    assert resultStatus.getProcessedOffset() == noOfRecords - 1;
    assert resultStatus.getLatestConsumerOffset()
        == topicPartitionChannel.getLatestConsumerOffset();
    assert resultStatus.getLatestConsumerOffset() == 0;

    assert resultStatus.getMetricsJmxReporter().getMetricRegistry().getMetrics().size()
        == SnowflakeTelemetryChannelStatus.NUM_METRICS;

    // verify telemetry was sent when channel closed
    topicPartitionChannel.closeChannel();
    Mockito.verify(this.mockTelemetryService, Mockito.times(1))
        .reportKafkaPartitionUsage(Mockito.any(SnowflakeTelemetryChannelStatus.class), eq(true));
    Mockito.verify(this.mockTelemetryService, Mockito.times(1))
        .reportKafkaPartitionStart(Mockito.any(SnowflakeTelemetryChannelCreation.class));
    assert topicPartitionChannel
            .getSnowflakeTelemetryChannelStatus()
            .getMetricsJmxReporter()
            .getMetricRegistry()
            .getMetrics()
            .size()
        == 0;
  }

  @Test
  public void testTopicPartitionChannelInvalidJmxReporter() throws Exception {
    // variables
    int noOfRecords = 5;

    // setup insert
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(mockStreamingChannel.close()).thenReturn(Mockito.mock(CompletableFuture.class));

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            this.mockStreamingClient,
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.enableSchematization,
            this.streamingBufferThreshold,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockSnowflakeConnectionService,
            new RecordService(),
            this.mockTelemetryService,
            true,
            null);

    // insert records
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, TOPIC, PARTITION);
    records.forEach(topicPartitionChannel::insertRecord);

    // verify no errors are thrown with invalid jmx reporter but enabled jmx monitoring
    SnowflakeTelemetryChannelStatus resultStatus =
        topicPartitionChannel.getSnowflakeTelemetryChannelStatus();
    assert resultStatus.getMetricsJmxReporter() == null;

    topicPartitionChannel.closeChannel();
    assert resultStatus.getMetricsJmxReporter() == null;
  }
}
