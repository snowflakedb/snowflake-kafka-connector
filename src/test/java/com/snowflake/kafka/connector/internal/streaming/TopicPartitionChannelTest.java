package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;

import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
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

  private static final String TOPIC = "TEST";

  private static final int PARTITION = 0;

  private static final String TEST_CHANNEL_NAME =
      SnowflakeSinkServiceV2.partitionChannelKey(TOPIC, PARTITION);
  private static final String TEST_DB = "TEST_DB";
  private static final String TEST_SC = "TEST_SC";
  private static final String TEST_TABLE_NAME = "TEST_TABLE";

  @Before
  public void setupEachTest() {
    Mockito.when(mockStreamingClient.isClosed()).thenReturn(false);
    Mockito.when(mockStreamingClient.openChannel(ArgumentMatchers.any(OpenChannelRequest.class)))
        .thenReturn(mockStreamingChannel);
    Mockito.when(mockStreamingChannel.getFullyQualifiedName()).thenReturn(TEST_CHANNEL_NAME);
  }

  @Test(expected = IllegalStateException.class)
  public void testTopicPartitionChannelInit_streamingClientClosed() {
    Mockito.when(mockStreamingClient.isClosed()).thenReturn(true);
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);
  }

  @Test
  public void testFetchOffsetTokenWithRetry_null() {
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn(null);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

    Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

  @Test
  public void testFetchOffsetTokenWithRetry_validLong() {

    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken()).thenReturn("100");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

    Assert.assertEquals(100L, topicPartitionChannel.fetchOffsetTokenWithRetry());
  }

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
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

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

    Assert.assertFalse(topicPartitionChannel.isPartitionBufferEmpty());

    // insert buffered rows by calling insertRows API. (Which we will mock)
    topicPartitionChannel.insertBufferedRows();

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
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

    topicPartitionChannel.closeChannel();
  }

  /* Only SFExceptions are retried and goes into fallback. */
  @Test(expected = SFException.class)
  public void testFetchOffsetTokenWithRetry_SFException() {
    SFException exception = new SFException(ErrorCode.INVALID_CHANNEL, "INVALID_CHANNEL");
    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenThrow(exception)
        .thenThrow(exception)
        .thenThrow(exception)
        .thenThrow(exception);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

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
    SFException exception = new SFException(ErrorCode.INVALID_CHANNEL, "INVALID_CHANNEL");
    final String offsetTokenAfterMaxAttempts = "0";

    Mockito.when(mockStreamingChannel.getLatestCommittedOffsetToken())
        .thenThrow(exception)
        .thenThrow(exception)
        .thenThrow(exception)
        .thenReturn(offsetTokenAfterMaxAttempts);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(
            mockStreamingClient,
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

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
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

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
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

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
            TEST_CHANNEL_NAME,
            TEST_DB,
            TEST_SC,
            TEST_TABLE_NAME,
            mockKafkaRecordErrorReporter);

    try {
      Assert.assertEquals(-1L, topicPartitionChannel.fetchOffsetTokenWithRetry());
    } catch (RuntimeException ex) {
      Mockito.verify(mockStreamingClient, Mockito.times(1)).openChannel(ArgumentMatchers.any());
      Mockito.verify(topicPartitionChannel.getChannel(), Mockito.times(1))
          .getLatestCommittedOffsetToken();
      throw ex;
    }
  }
}
