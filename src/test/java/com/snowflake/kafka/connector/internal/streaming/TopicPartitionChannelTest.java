package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import com.codahale.metrics.MetricRegistry;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.RecordServiceFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

  @Mock private SchemaEvolutionService schemaEvolutionService;

  private static final String TOPIC = "TEST";

  private static final int PARTITION = 0;

  private static final String TEST_CHANNEL_NAME =
      SnowflakeSinkServiceV2.partitionChannelKey(TOPIC, PARTITION);
  private static final String TEST_TABLE_NAME = "TEST_TABLE";

  private TopicPartition topicPartition;

  private Map<String, String> sfConnectorConfig;

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
    this.sfConnectorConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
        Boolean.toString(this.enableSchematization));
  }

  @Test(expected = IllegalStateException.class)
  public void testTopicPartitionChannelInit_streamingClientClosed() {
    Mockito.when(mockStreamingClient.isClosed()).thenReturn(true);
    createTopicPartitionChannel(
        mockStreamingClient,
        topicPartition,
        TEST_CHANNEL_NAME,
        TEST_TABLE_NAME,
        sfConnectorConfig,
        mockKafkaRecordErrorReporter,
        mockSinkTaskContext,
        mockSnowflakeConnectionService,
        mockTelemetryService,
        this.schemaEvolutionService);
  }

  @Test
  public void testFetchOffsetTokenWithRetry_null() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

    Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

  @Test
  public void testFetchOffsetTokenWithRetry_validLong() {

    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn("100");

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

    Assert.assertEquals(100L, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

  // TODO:: Fix this test
  @Test
  public void testFirstRecordForChannel() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(mockStreamingChannel.insertRow(anyMap(), anyString()))
        .thenReturn(new InsertValidationResponse());

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

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

    topicPartitionChannel.insertRecord(record1, true);

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
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);

    topicPartitionChannel.closeChannel();
  }

  @Test
  public void closeChannel_withSFExceptionInFuture_swallowsException() {
    // given
    CompletableFuture<Void> closeChannelFuture = new CompletableFuture<>();
    closeChannelFuture.completeExceptionally(SF_EXCEPTION);

    Mockito.when(mockStreamingChannel.close()).thenReturn(closeChannelFuture);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);

    // when
    assertDoesNotThrow(topicPartitionChannel::closeChannel);

    // then
    Mockito.verify(mockTelemetryService, Mockito.times(1))
        .reportKafkaConnectFatalError(anyString());
  }

  @Test
  public void closeChannel_withSFExceptionThrown_swallowsException() {
    // given
    Mockito.when(mockStreamingChannel.close()).thenThrow(SF_EXCEPTION);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);

    // when
    assertDoesNotThrow(topicPartitionChannel::closeChannel);

    // then
    Mockito.verify(mockTelemetryService, Mockito.times(1))
        .reportKafkaConnectFatalError(anyString());
  }

  @Test
  public void closeChannelAsync_withNotSFExceptionInFuture_propagatesException() {
    // given
    RuntimeException cause = new RuntimeException();

    CompletableFuture<Void> closeChannelFuture = new CompletableFuture<>();
    closeChannelFuture.completeExceptionally(cause);

    Mockito.when(mockStreamingChannel.close()).thenReturn(closeChannelFuture);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);

    // when
    ExecutionException ex =
        assertThrows(
            ExecutionException.class, () -> topicPartitionChannel.closeChannelAsync().get());

    // then
    assertSame(cause, ex.getCause());

    Mockito.verify(mockTelemetryService, Mockito.times(1))
        .reportKafkaConnectFatalError(anyString());
  }

  @Test
  public void closeChannelAsync_withSFExceptionInFuture_swallowsException() {
    // given
    CompletableFuture<Void> closeChannelFuture = new CompletableFuture<>();
    closeChannelFuture.completeExceptionally(SF_EXCEPTION);

    Mockito.when(mockStreamingChannel.close()).thenReturn(closeChannelFuture);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);

    // when
    assertDoesNotThrow(() -> topicPartitionChannel.closeChannelAsync().get());

    // then
    Mockito.verify(mockTelemetryService, Mockito.times(1))
        .reportKafkaConnectFatalError(anyString());
  }

  @Test
  public void closeChannelAsync_withSFExceptionThrown_swallowsException() {
    // given
    Mockito.when(mockStreamingChannel.close()).thenThrow(SF_EXCEPTION);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);

    // when
    assertDoesNotThrow(() -> topicPartitionChannel.closeChannelAsync().get());

    // then
    Mockito.verify(mockTelemetryService, Mockito.times(1))
        .reportKafkaConnectFatalError(anyString());
  }

  @Test
  public void testStreamingChannelMigrationEnabledAndDisabled() {

    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);
    Mockito.when(
            mockSnowflakeConnectionService.migrateStreamingChannelOffsetToken(
                anyString(), anyString(), Mockito.anyString()))
        .thenReturn(new ChannelMigrateOffsetTokenResponseDTO(50, "SUCCESS"));

    // checking default
    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);
    Mockito.verify(mockSnowflakeConnectionService, Mockito.times(1))
        .migrateStreamingChannelOffsetToken(anyString(), anyString(), anyString());

    Map<String, String> customSfConfig = new HashMap<>(sfConnectorConfig);
    customSfConfig.put(ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG, "true");

    topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            customSfConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);
    Mockito.verify(mockSnowflakeConnectionService, Mockito.times(2))
        .migrateStreamingChannelOffsetToken(anyString(), anyString(), anyString());

    customSfConfig.put(ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG, "false");
    SnowflakeConnectionService anotherMockForParamDisabled =
        Mockito.mock(SnowflakeConnectionService.class);

    topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            true,
            customSfConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            anotherMockForParamDisabled,
            RecordServiceFactory.createRecordService(false, false),
            mockTelemetryService,
            false,
            null,
            this.schemaEvolutionService);
    Mockito.verify(anotherMockForParamDisabled, Mockito.times(0))
        .migrateStreamingChannelOffsetToken(anyString(), anyString(), anyString());
  }

  @Test
  public void testStreamingChannelMigration_InvalidResponse() {

    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);
    Mockito.when(
            mockSnowflakeConnectionService.migrateStreamingChannelOffsetToken(
                anyString(), anyString(), Mockito.anyString()))
        .thenThrow(new RuntimeException("Exception migrating channel offset token"));
    try {
      // checking default
      TopicPartitionChannel topicPartitionChannel =
          createTopicPartitionChannel(
              mockStreamingClient,
              topicPartition,
              TEST_CHANNEL_NAME,
              TEST_TABLE_NAME,
              true,
              sfConnectorConfig,
              mockKafkaRecordErrorReporter,
              mockSinkTaskContext,
              mockSnowflakeConnectionService,
              RecordServiceFactory.createRecordService(false, false),
              mockTelemetryService,
              false,
              null,
              this.schemaEvolutionService);
      Assert.fail("Should throw an exception:");
    } catch (Exception e) {
      Mockito.verify(mockSnowflakeConnectionService, Mockito.times(1))
          .migrateStreamingChannelOffsetToken(anyString(), anyString(), anyString());
      assert e.getMessage().contains("Exception migrating channel offset token");
    }
  }

  /* Only SFExceptions are retried and goes into fallback. */
  @Test(expected = SFException.class)
  public void testFetchOffsetTokenWithRetry_SFException() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenThrow(SF_EXCEPTION);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

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
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

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
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

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
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

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
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);

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
    final int noOfRecords = 5;
    int expectedInsertRowsCount = 0;
    int expectedOpenChannelCount = 0;
    int expectedGetOffsetCount = 0;

    // setup mocks to fail first insert and return two null snowflake offsets (open channel and
    // failed insert) before succeeding
    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenThrow(SF_EXCEPTION)
        .thenReturn(new InsertValidationResponse());
    Mockito.when(mockStreamingChannel.insertRow(anyMap(), anyString()))
        .thenThrow(SF_EXCEPTION)
        .thenReturn(new InsertValidationResponse());
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenReturn(null)
        .thenReturn(null)
        .thenReturn(Long.toString(noOfRecords - 1));

    // create tpchannel
    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            mockStreamingClient,
            topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            sfConnectorConfig,
            mockKafkaRecordErrorReporter,
            mockSinkTaskContext,
            mockSnowflakeConnectionService,
            mockTelemetryService,
            this.schemaEvolutionService);
    expectedOpenChannelCount++;
    expectedGetOffsetCount++;

    // verify initial mock counts after tpchannel creation
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(expectedInsertRowsCount))
        .insertRows(
            ArgumentMatchers.any(Iterable.class),
            ArgumentMatchers.any(String.class),
            ArgumentMatchers.any(String.class));
    Mockito.verify(mockStreamingClient, Mockito.times(expectedOpenChannelCount))
        .openChannel(ArgumentMatchers.any());
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(expectedGetOffsetCount))
        .getLatestCommittedOffsetToken();

    // Test inserting record 0, which should fail to ingest so the other records are ignored
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, TOPIC, PARTITION);
    for (int idx = 0; idx < records.size(); idx++) {
      topicPartitionChannel.insertRecord(records.get(idx), idx == 0);
    }
    expectedInsertRowsCount++;
    expectedOpenChannelCount++;
    expectedGetOffsetCount++;

    // verify mocks only tried ingesting once
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(expectedInsertRowsCount))
        .insertRow(anyMap(), anyString());

    Mockito.verify(mockStreamingClient, Mockito.times(expectedOpenChannelCount))
        .openChannel(ArgumentMatchers.any());
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(expectedGetOffsetCount))
        .getLatestCommittedOffsetToken();

    // Retry the insert again, now everything should be ingested and the offset token should be
    // noOfRecords-1
    for (int idx = 0; idx < records.size(); idx++) {
      topicPartitionChannel.insertRecord(records.get(idx), idx == 0);
    }
    Assert.assertEquals(noOfRecords - 1, topicPartitionChannel.fetchOffsetTokenWithRetry());
    expectedInsertRowsCount += noOfRecords;
    expectedGetOffsetCount++;

    // verify mocks ingested each record
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(6))
        .insertRow(anyMap(), anyString());
    Mockito.verify(mockStreamingClient, Mockito.times(expectedOpenChannelCount))
        .openChannel(ArgumentMatchers.any());
    Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(expectedGetOffsetCount))
        .getLatestCommittedOffsetToken();
  }

  @Test
  public void testInsertRowsWithSchemaEvolution() {
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

      Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn("0");

      SnowflakeConnectionService conn = Mockito.mock(SnowflakeConnectionService.class);
      Mockito.when(
              conn.hasSchemaEvolutionPermission(ArgumentMatchers.any(), ArgumentMatchers.any()))
          .thenReturn(true);
      Mockito.doNothing()
          .when(conn)
          .appendColumnsToTable(ArgumentMatchers.any(), ArgumentMatchers.any());

      Map<String, String> sfConnectorConfigWithErrors = new HashMap<>(sfConnectorConfig);
      sfConnectorConfigWithErrors.put(
          ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
      sfConnectorConfigWithErrors.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");
      InMemoryKafkaRecordErrorReporter kafkaRecordErrorReporter =
          new InMemoryKafkaRecordErrorReporter();

      TopicPartitionChannel topicPartitionChannel =
          createTopicPartitionChannel(
              mockStreamingClient,
              topicPartition,
              TEST_CHANNEL_NAME,
              TEST_TABLE_NAME,
              this.enableSchematization,
              sfConnectorConfigWithErrors,
              kafkaRecordErrorReporter,
              mockSinkTaskContext,
              conn,
              RecordServiceFactory.createRecordService(false, false),
              mockTelemetryService,
              false,
              null,
              this.schemaEvolutionService);

      final int noOfRecords = 2;
      List<SinkRecord> records =
          TestUtils.createNativeJsonSinkRecords(1, noOfRecords, TOPIC, PARTITION);

      for (int idx = 0; idx < records.size(); idx++) {
        topicPartitionChannel.insertRecord(records.get(idx), idx == 0);
      }

      // Verify that the buffer is cleaned up and one record is in the DLQ
      Assert.assertTrue(topicPartitionChannel.isPartitionBufferEmpty());
      Assert.assertEquals(1, kafkaRecordErrorReporter.getReportedRecords().size());
    }
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
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(mockStreamingChannel.close()).thenReturn(Mockito.mock(CompletableFuture.class));

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            this.mockStreamingClient,
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.enableSchematization,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            this.mockTelemetryService,
            true,
            metricsJmxReporter,
            this.schemaEvolutionService);

    // insert records
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, TOPIC, PARTITION);
    for (int idx = 0; idx < records.size(); idx++) {
      topicPartitionChannel.insertRecord(records.get(idx), idx == 0);
    }

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
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(
            mockStreamingChannel.insertRow(
                ArgumentMatchers.any(Map.class), ArgumentMatchers.any(String.class)))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(mockStreamingChannel.close()).thenReturn(Mockito.mock(CompletableFuture.class));

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            this.mockStreamingClient,
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.enableSchematization,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            this.mockTelemetryService,
            true,
            null,
            this.schemaEvolutionService);

    // insert records
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, TOPIC, PARTITION);
    for (int idx = 0; idx < records.size(); idx++) {
      topicPartitionChannel.insertRecord(records.get(idx), idx == 0);
    }

    // verify no errors are thrown with invalid jmx reporter but enabled jmx monitoring
    SnowflakeTelemetryChannelStatus resultStatus =
        topicPartitionChannel.getSnowflakeTelemetryChannelStatus();
    assert resultStatus.getMetricsJmxReporter() == null;

    topicPartitionChannel.closeChannel();
    assert resultStatus.getMetricsJmxReporter() == null;
  }

  public TopicPartitionChannel createTopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelNameFormatV1,
      final String tableName,
      final Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext,
      SnowflakeConnectionService conn,
      SnowflakeTelemetryService telemetryService,
      SchemaEvolutionService schemaEvolutionService) {
    return new DirectTopicPartitionChannel(
        streamingIngestClient,
        topicPartition,
        channelNameFormatV1,
        tableName,
        sfConnectorConfig,
        kafkaRecordErrorReporter,
        sinkTaskContext,
        conn,
        telemetryService,
        this.schemaEvolutionService,
        new InsertErrorMapper());
  }

  public TopicPartitionChannel createTopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelNameFormatV1,
      final String tableName,
      boolean hasSchemaEvolutionPermission,
      final Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext,
      SnowflakeConnectionService conn,
      RecordService recordService,
      SnowflakeTelemetryService telemetryService,
      boolean enableCustomJMXMonitoring,
      MetricsJmxReporter metricsJmxReporter,
      SchemaEvolutionService schemaEvolutionService) {
    return new DirectTopicPartitionChannel(
        streamingIngestClient,
        topicPartition,
        channelNameFormatV1,
        tableName,
        hasSchemaEvolutionPermission,
        sfConnectorConfig,
        kafkaRecordErrorReporter,
        sinkTaskContext,
        conn,
        recordService,
        telemetryService,
        enableCustomJMXMonitoring,
        metricsJmxReporter,
        this.schemaEvolutionService,
        new InsertErrorMapper());
  }

  @Test
  public void testOffsetTokenVerificationFunction() {
    Assert.assertTrue(StreamingUtils.offsetTokenVerificationFunction.verify("1", "2", "4", 2));
    Assert.assertTrue(StreamingUtils.offsetTokenVerificationFunction.verify("1", "2", null, 1));
    Assert.assertTrue(StreamingUtils.offsetTokenVerificationFunction.verify(null, null, null, 0));
    Assert.assertTrue(StreamingUtils.offsetTokenVerificationFunction.verify("1", "3", "4", 3));
    Assert.assertFalse(StreamingUtils.offsetTokenVerificationFunction.verify("2", "1", "4", 3));
  }

  @Test
  public void assignANewChannelAfterTheSetupIsFullyDone() throws Exception {
    // given
    String noOffset = "-1";

    SnowflakeStreamingIngestChannel channel1 = Mockito.mock(SnowflakeStreamingIngestChannel.class);
    Mockito.when(channel1.getLatestCommittedOffsetToken())
        .thenReturn(noOffset)
        .thenThrow(new SFException(ErrorCode.CHANNEL_STATUS_INVALID));

    Mockito.when(channel1.insertRow(anyMap(), anyString()))
        .thenThrow(new SFException(ErrorCode.CHANNEL_STATUS_INVALID));
    Mockito.when(channel1.insertRows(anyIterable(), anyString(), anyString()))
        .thenThrow(new SFException(ErrorCode.CHANNEL_STATUS_INVALID));

    SnowflakeStreamingIngestChannel channel2 = Mockito.mock(SnowflakeStreamingIngestChannel.class);
    Mockito.when(channel2.getLatestCommittedOffsetToken())
        .thenThrow(new SFException(ErrorCode.IO_ERROR));
    Mockito.when(channel2.insertRow(anyMap(), anyString()))
        .thenReturn(new InsertValidationResponse());
    Mockito.when(channel2.insertRows(anyIterable(), anyString(), anyString()))
        .thenReturn(new InsertValidationResponse());

    Mockito.when(mockStreamingClient.openChannel(any(OpenChannelRequest.class)))
        .thenReturn(channel1, channel2);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            this.mockStreamingClient,
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.enableSchematization,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            this.mockTelemetryService,
            true,
            null,
            this.schemaEvolutionService);

    // expect
    Assert.assertThrows(
        SFException.class, () -> topicPartitionChannel.getOffsetSafeToCommitToKafka());

    // when
    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 2, TOPIC, PARTITION);

    // expect
    Assert.assertThrows(SFException.class, () -> insertAndFlush(topicPartitionChannel, records));
  }

  private void insertAndFlush(TopicPartitionChannel channel, List<SinkRecord> records) {
    for (int idx = 0; idx < records.size(); idx++) {
      channel.insertRecord(records.get(idx), idx == 0);
    }
  }

  @Test
  public void assignANewChannel_whenNoOffsetIsPresentInSnowflake() {
    // given
    String noOffset = "-1";

    SnowflakeStreamingIngestChannel originalChannel =
        Mockito.mock(SnowflakeStreamingIngestChannel.class);
    Mockito.when(originalChannel.getLatestCommittedOffsetToken())
        .thenReturn(noOffset)
        .thenThrow(new SFException(ErrorCode.CHANNEL_STATUS_INVALID));

    SnowflakeStreamingIngestChannel reopenedChannel =
        Mockito.mock(SnowflakeStreamingIngestChannel.class);
    Mockito.when(reopenedChannel.getLatestCommittedOffsetToken()).thenReturn(noOffset);

    Mockito.when(mockStreamingClient.openChannel(any(OpenChannelRequest.class)))
        .thenReturn(originalChannel, reopenedChannel);

    TopicPartitionChannel topicPartitionChannel =
        createTopicPartitionChannel(
            this.mockStreamingClient,
            this.topicPartition,
            TEST_CHANNEL_NAME,
            TEST_TABLE_NAME,
            this.enableSchematization,
            this.sfConnectorConfig,
            this.mockKafkaRecordErrorReporter,
            this.mockSinkTaskContext,
            this.mockSnowflakeConnectionService,
            RecordServiceFactory.createRecordService(false, false),
            this.mockTelemetryService,
            true,
            null,
            this.schemaEvolutionService);

    // when
    topicPartitionChannel.getOffsetSafeToCommitToKafka();

    // then
    Assert.assertEquals(reopenedChannel, topicPartitionChannel.getChannel());
  }
}
