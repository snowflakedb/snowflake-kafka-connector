package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicPartitionChannelTest {

  @Mock private KafkaRecordErrorReporter mockKafkaRecordErrorReporter;

  private SnowflakeStreamingIngestChannel mockStreamingChannel;

  private final String streamingChannelName =
      SnowflakeSinkServiceV2.partitionChannelKey(TOPIC, PARTITION);

  private static final String TOPIC = "TEST";

  private static final int PARTITION = 0;

  private static final String TEST_DB = "TEST_DB";
  private static final String TEST_SC = "TEST_SC";

  @Test
  public void testFetchLastCommittedOffsetToken_null() {

    mockStreamingChannel = new MockStreamingIngestChannel(() -> null);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(mockStreamingChannel, mockKafkaRecordErrorReporter);

    Assert.assertEquals(-1L, topicPartitionChannel.fetchLatestCommittedOffsetFromSnowflake());
  }

  @Test
  public void testFetchLastCommittedOffsetToken_validLong() {

    mockStreamingChannel = new MockStreamingIngestChannel(() -> "100");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(mockStreamingChannel, mockKafkaRecordErrorReporter);

    Assert.assertEquals(100L, topicPartitionChannel.fetchLatestCommittedOffsetFromSnowflake());
  }

  @Test(expected = ConnectException.class)
  public void testFetchLastCommittedOffsetToken_InvalidNumber() {

    mockStreamingChannel = new MockStreamingIngestChannel(() -> "invalidNo");

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(mockStreamingChannel, mockKafkaRecordErrorReporter);

    try {
      topicPartitionChannel.fetchLatestCommittedOffsetFromSnowflake();
      Assert.fail("Should throw exception");
    } catch (ConnectException exception) {
      Assert.assertTrue(exception.getMessage().contains("invalidNo"));
      throw exception;
    }
  }

  @Test
  public void testFirstRecordForChannel() {
    mockStreamingChannel =
        new MockStreamingIngestChannel(() -> new InsertValidationResponse(), () -> null);

    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(mockStreamingChannel, mockKafkaRecordErrorReporter);
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

    // After insertRows API is called, the offsetToken would return 0

    mockStreamingChannel =
        new MockStreamingIngestChannel(() -> new InsertValidationResponse(), () -> "0");
  }

  @Test
  public void testCloseChannelException() throws Exception {
    SnowflakeStreamingIngestChannel mockChannel =
        Mockito.mock(SnowflakeStreamingIngestChannel.class);
    CompletableFuture mockFuture = Mockito.mock(CompletableFuture.class);

    Mockito.when(mockChannel.close()).thenReturn(mockFuture);

    Mockito.when(mockFuture.get()).thenThrow(new InterruptedException("Interrupted Exception"));
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(mockChannel, mockKafkaRecordErrorReporter);

    topicPartitionChannel.closeChannel();
  }

  private class MockStreamingIngestChannel implements SnowflakeStreamingIngestChannel {

    Supplier<InsertValidationResponse> insertRowValidationResponseSupplier;

    Supplier<String> getOffsetTokenSupplier;

    MockStreamingIngestChannel(Supplier<String> getOffsetTokenSupplier) {
      this(null, getOffsetTokenSupplier);
    }

    MockStreamingIngestChannel(
        Supplier<InsertValidationResponse> insertRowValidationResponseSupplier,
        Supplier<String> getOffsetTokenSupplier) {
      this.insertRowValidationResponseSupplier = insertRowValidationResponseSupplier;
      this.getOffsetTokenSupplier = getOffsetTokenSupplier;
    }

    @Override
    public String getFullyQualifiedName() {
      return streamingChannelName;
    }

    @Override
    public String getName() {
      return streamingChannelName;
    }

    @Override
    public String getDBName() {
      return TEST_DB;
    }

    @Override
    public String getSchemaName() {
      return TEST_SC;
    }

    @Override
    public String getTableName() {
      return TOPIC;
    }

    @Override
    public String getFullyQualifiedTableName() {
      return null;
    }

    @Override
    public boolean isValid() {
      return true;
    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public CompletableFuture<Void> close() {
      return null;
    }

    @Override
    public InsertValidationResponse insertRow(Map<String, Object> map, @Nullable String s) {
      return insertRowValidationResponseSupplier.get();
    }

    @Override
    public InsertValidationResponse insertRows(
        Iterable<Map<String, Object>> iterable, @Nullable String s) {
      return insertRowValidationResponseSupplier.get();
    }

    @Override
    public String getLatestCommittedOffsetToken() {
      return getOffsetTokenSupplier.get();
    }
  }
}
