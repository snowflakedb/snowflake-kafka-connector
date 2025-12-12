package com.snowflake.kafka.connector.internal.streaming.v2;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for SnowflakeStreamingIngestClient and SnowflakeStreamingIngestChannel contract
 * behavior.
 */
class SnowflakeStreamingIngestClientContractIT {

  private static final String CLIENT_1_NAME = "test_client_1";
  private static final String CLIENT_2_NAME = "test_client_2";

  private Map<String, String> connectorConfig;
  private StreamingClientProperties streamingClientProperties;
  private String tableName;
  private String pipeName;
  private String channelName;

  private SnowflakeStreamingIngestClient client1;
  private SnowflakeStreamingIngestClient client2;
  private SnowflakeConnectionService connectionService;

  @BeforeEach
  void setUp() {
    final long salt = System.currentTimeMillis();
    connectionService = TestUtils.getConnectionServiceWithEncryptedKey();
    connectorConfig = TestUtils.getConnectorConfigurationForStreaming(true);
    streamingClientProperties = new StreamingClientProperties(connectorConfig);

    tableName = "contract_test_table_" + salt;
    pipeName = tableName + "_STREAMING";
    channelName = "channel_" + salt;
    connectionService.executeQueryWithParameters(
        format("create or replace table %s (record_metadata variant, f1 varchar)", tableName));

    final String dbName = Utils.getDatabase(connectorConfig);
    final String schemaName = Utils.getSchema(connectorConfig);
    final Properties clientProperties = StreamingClientManager.getClientProperties(connectorConfig);

    client1 =
        SnowflakeStreamingIngestClientFactory.builder(
                CLIENT_1_NAME + "_" + salt, dbName, schemaName, pipeName)
            .setProperties(clientProperties)
            .setParameterOverrides(streamingClientProperties.parameterOverrides)
            .build();

    client2 =
        SnowflakeStreamingIngestClientFactory.builder(
                CLIENT_2_NAME + "_" + salt, dbName, schemaName, pipeName)
            .setProperties(clientProperties)
            .setParameterOverrides(streamingClientProperties.parameterOverrides)
            .build();
  }

  @AfterEach
  void tearDown() {
    closeClientSafely(client1);
    closeClientSafely(client2);
    TestUtils.dropTable(tableName);
  }

  /**
   * Test 1: What happens when you open channel ch1 using c1, then close it using c2, and then call
   * getChannelStatus on ch1 that comes from c1.
   *
   * <p>Expected behavior: The channel ch1 from c1 becomes invalidated when c2 closes the channel
   * with the same name. Calling getChannelStatus on the invalidated channel should reflect this.
   */
  @Test
  void testOpenChannelWithC1_CloseWithC2_GetChannelStatusOnCh1FromC1() {
    // Given: Open channel using client1
    final OpenChannelResult result1 = client1.openChannel(channelName, null);
    final SnowflakeStreamingIngestChannel ch1FromC1 = result1.getChannel();

    assertThat(result1.getChannelStatus().getStatusCode()).isEqualTo("SUCCESS");
    assertThat(ch1FromC1.isClosed()).isFalse();

    // When: Client2 opens the same channel (which takes ownership)
    final OpenChannelResult result2 = client2.openChannel(channelName, null);
    final SnowflakeStreamingIngestChannel ch1FromC2 = result2.getChannel();

    assertThat(result2.getChannelStatus().getStatusCode()).isEqualTo("SUCCESS");

    // Then: Close the channel using client2
    ch1FromC2.close();

    // Assert: Check channel status on the original ch1 from c1
    // The channel from c1 should detect it's no longer the owner
    final ChannelStatus statusFromC1 = ch1FromC1.getChannelStatus();

    // The status should indicate the channel is no longer valid for this client
    // or the channel was invalidated
    assertThat(statusFromC1).isNotNull();
    // Log the status for observability
    System.out.println(
        "Channel status from c1 after c2 close: statusCode="
            + statusFromC1.getStatusCode()
            + ", channelName="
            + statusFromC1.getChannelName());
  }

  /**
   * Test 2: What happens when you create a channel ch1 using c1, use it, drop it using c2, then try
   * using the same instance ch1 and append rows.
   *
   * <p>Expected behavior: After the channel is dropped using c2, attempting to append rows using
   * the ch1 instance from c1 should fail with an exception.
   */
  @Test
  void testCreateChannelWithC1_UseIt_DropWithC2_TryAppendRows() {
    // Given: Open channel using client1 and append some rows
    final OpenChannelResult result1 = client1.openChannel(channelName, null);
    final SnowflakeStreamingIngestChannel ch1FromC1 = result1.getChannel();

    assertThat(result1.getChannelStatus().getStatusCode()).isEqualTo("SUCCESS");

    // Append a row to ensure the channel is working
    final Map<String, Object> row1 = createTestRow("value1");
    ch1FromC1.appendRow(row1, "0");

    // When: Drop the channel using client2
    client2.dropChannel(channelName);

    // Then: Try to append rows using the original ch1 from c1
    final Map<String, Object> row2 = createTestRow("value2");

    // This should throw an exception because the channel was dropped
    assertThatThrownBy(() -> ch1FromC1.appendRow(row2, "1"))
        .isInstanceOf(SFException.class)
        .satisfies(
            ex -> {
              System.out.println(
                  "Exception when appending to dropped channel: "
                      + ex.getClass().getSimpleName()
                      + " - "
                      + ex.getMessage());
            });
  }

  /**
   * Test 3: What happens when you open a channel using c1 and then you open it again using c1.
   *
   * <p>Expected behavior: Opening the same channel again with the same client should succeed. The
   * second open call should return a new channel instance, and the first channel should become
   * invalidated.
   */
  @Test
  void testOpenChannelWithC1_ThenOpenAgainWithC1() {
    // Given: Open channel using client1
    final OpenChannelResult result1 = client1.openChannel(channelName, null);
    final SnowflakeStreamingIngestChannel ch1First = result1.getChannel();

    assertThat(result1.getChannelStatus().getStatusCode()).isEqualTo("SUCCESS");
    assertThat(ch1First.isClosed()).isFalse();

    // Append a row to the first channel
    final Map<String, Object> row1 = createTestRow("value1");
    ch1First.appendRow(row1, "0");

    // When: Open the same channel again using client1
    final OpenChannelResult result2 = client1.openChannel(channelName, null);
    final SnowflakeStreamingIngestChannel ch1Second = result2.getChannel();

    assertThat(result2.getChannelStatus().getStatusCode()).isEqualTo("SUCCESS");

    // Then: The second channel should be a different instance
    assertThat(ch1Second).isNotSameAs(ch1First);

    // The second channel should work
    final Map<String, Object> row2 = createTestRow("value2");
    ch1Second.appendRow(row2, "1");

    // Try to use the first channel - it should be invalidated
    final Map<String, Object> row3 = createTestRow("value3");
    assertThatThrownBy(() -> ch1First.appendRow(row3, "2"))
        .isInstanceOf(SFException.class)
        .satisfies(
            ex -> {
              System.out.println(
                  "Exception when using first channel after re-open: "
                      + ex.getClass().getSimpleName()
                      + " - "
                      + ex.getMessage());
            });
  }

  /**
   * Test 4: What happens when you open a channel using c1 and then you open the same channel name
   * using c2.
   *
   * <p>Expected behavior: Opening the same channel name with a different client (c2) should succeed
   * and transfer ownership to c2. The channel from c1 should become invalidated, and subsequent
   * operations on it should fail.
   */
  @Test
  void testOpenChannelWithC1_ThenOpenSameChannelNameWithC2() {
    // Given: Open channel using client1
    final OpenChannelResult result1 = client1.openChannel(channelName, null);
    final SnowflakeStreamingIngestChannel ch1FromC1 = result1.getChannel();

    assertThat(result1.getChannelStatus().getStatusCode()).isEqualTo("SUCCESS");
    assertThat(ch1FromC1.isClosed()).isFalse();

    // Append a row using client1's channel
    final Map<String, Object> row1 = createTestRow("value1");
    ch1FromC1.appendRow(row1, "0");

    // When: Open the same channel using client2
    final OpenChannelResult result2 = client2.openChannel(channelName, null);
    final SnowflakeStreamingIngestChannel ch1FromC2 = result2.getChannel();

    assertThat(result2.getChannelStatus().getStatusCode()).isEqualTo("SUCCESS");
    assertThat(ch1FromC2.isClosed()).isFalse();

    // Then: The channel from c2 should work
    final Map<String, Object> row2 = createTestRow("value2");
    ch1FromC2.appendRow(row2, "1");

    // Try to use the channel from c1 - it should be invalidated
    final Map<String, Object> row3 = createTestRow("value3");
    assertThatThrownBy(() -> ch1FromC1.appendRow(row3, "2"))
        .isInstanceOf(SFException.class)
        .satisfies(
            ex -> {
              System.out.println(
                  "Exception when using c1 channel after c2 opens same channel: "
                      + ex.getClass().getSimpleName()
                      + " - "
                      + ex.getMessage());
            });

    // Verify c2's channel still works
    final Map<String, Object> row4 = createTestRow("value4");
    ch1FromC2.appendRow(row4, "3"); // Should succeed

    // Verify channel status from both clients
    final ChannelStatus statusFromC1 = ch1FromC1.getChannelStatus();
    final ChannelStatus statusFromC2 = ch1FromC2.getChannelStatus();

    System.out.println("Status from c1 channel: " + statusFromC1.getStatusCode());
    System.out.println("Status from c2 channel: " + statusFromC2.getStatusCode());
  }

  private Map<String, Object> createTestRow(final String value) {
    final Map<String, Object> row = new HashMap<>();
    row.put("RECORD_CONTENT", "{\"test\": \"" + value + "\"}");
    return row;
  }

  private void closeClientSafely(final SnowflakeStreamingIngestClient client) {
    if (client != null) {
      try {
        client.close();
      } catch (final Exception e) {
        System.err.println("Error closing client: " + e.getMessage());
      }
    }
  }
}
