package com.snowflake.kafka.connector.internal.streaming;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicPartitionChannelTest {

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

    TopicPartitionChannel topicPartitionChannel = new TopicPartitionChannel(mockStreamingChannel);

    topicPartitionChannel.fetchLastCommittedOffsetToken();

    Assert.assertEquals(-1L, topicPartitionChannel.getOffsetPersistedInSnowflake());
  }

  @Test
  public void testFetchLastCommittedOffsetToken_validLong() {

    mockStreamingChannel = new MockStreamingIngestChannel(() -> "100");

    TopicPartitionChannel topicPartitionChannel = new TopicPartitionChannel(mockStreamingChannel);

    topicPartitionChannel.fetchLastCommittedOffsetToken();

    Assert.assertEquals(100L, topicPartitionChannel.getOffsetPersistedInSnowflake());
  }

  @Test(expected = ConnectException.class)
  public void testFetchLastCommittedOffsetToken_InvalidNumber() {

    mockStreamingChannel = new MockStreamingIngestChannel(() -> "invalidNo");

    TopicPartitionChannel topicPartitionChannel = new TopicPartitionChannel(mockStreamingChannel);

    try {
      topicPartitionChannel.fetchLastCommittedOffsetToken();
      Assert.fail("Should throw exception");
    } catch (ConnectException exception) {
      Assert.assertTrue(exception.getMessage().contains("invalidNo"));
      throw exception;
    }
  }

  private class MockStreamingIngestChannel implements SnowflakeStreamingIngestChannel {

    Supplier<String> getOffsetTokenSupplier;

    MockStreamingIngestChannel(Supplier<String> getOffsetTokenSupplier) {
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
      return null;
    }

    @Override
    public InsertValidationResponse insertRows(
        Iterable<Map<String, Object>> iterable, @Nullable String s) {
      return null;
    }

    @Override
    public String getLatestCommittedOffsetToken() {
      return getOffsetTokenSupplier.get();
    }
  }
}
