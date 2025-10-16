package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.*;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceV1;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class StreamingIngestClientV2ProviderIT {

  private static final KCLogger LOGGER =
      new KCLogger(StreamingIngestClientV2ProviderIT.class.getName());

  // Shared resources created once for the entire test class
  private static Map<String, String> connectorConfig;
  private static StreamingClientProperties streamingClientProperties;
  private static SnowflakeConnectionServiceV1 connectionService;
  private static String testTableName;
  private static String testTableName2;
  private static String testPipeName;
  private static String testPipeName2;

  // Provider instance reset before each test
  private StreamingIngestClientV2Provider provider;

  @BeforeAll
  public static void setUpClass() {
    // Create shared configuration and connection service once for all tests
    connectorConfig = TestUtils.getConfForStreaming();
    streamingClientProperties = new StreamingClientProperties(connectorConfig);
    connectionService = (SnowflakeConnectionServiceV1) TestUtils.getConnectionServiceForStreaming();

    // Create test tables (both to avoid creating them repeatedly)
    testTableName = "test_table_" + System.currentTimeMillis();
    testTableName2 = "test_table_2_" + System.currentTimeMillis();

    connectionService.createTable(testTableName);
    connectionService.createTable(testTableName2);

    // Generate pipe names using PipeNameProvider
    testPipeName = PipeNameProvider.pipeName(connectorConfig, testTableName);
    testPipeName2 = PipeNameProvider.pipeName(connectorConfig, testTableName2);

    // Create both pipes using SSv2PipeCreator
    SSv2PipeCreator pipeCreator1 =
        new SSv2PipeCreator(connectionService, testPipeName, testTableName);
    SSv2PipeCreator pipeCreator2 =
        new SSv2PipeCreator(connectionService, testPipeName2, testTableName2);

    pipeCreator1.createPipeIfNotExists();
    pipeCreator2.createPipeIfNotExists();
  }

  @BeforeEach
  public void setUp() {
    // Create a fresh provider instance for each test to ensure test isolation
    provider = new StreamingIngestClientV2Provider();
  }

  @AfterEach
  public void tearDown() {
    // Close all clients in the provider after each test to ensure clean state
    provider.closeAll();
  }

  @AfterAll
  public static void tearDownClass() {
    // Clean up shared Snowflake resources once at the end
    if (connectionService != null) {
      try {
        connectionService.dropPipe(testPipeName);
        connectionService.dropPipe(testPipeName2);
        TestUtils.dropTable(testTableName);
        TestUtils.dropTable(testTableName2);
        // Close connection service
        connectionService.close();
      } catch (Exception e) {
        // Log error but don't fail the test
        LOGGER.warn("Error during class tearDown: {}", e.getMessage());
      }
    }
  }

  @Test
  public void testGetClient_FirstTime_CreatesNewClient() {
    // When
    StreamingIngestClientV2Wrapper client =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);

    // Then
    assertThat(client).as("Client should not be null").isNotNull();
  }

  @Test
  public void testGetClient_SamePipeName_ReturnsExistingClient() {
    // Given
    StreamingIngestClientV2Wrapper client1 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);

    // When
    StreamingIngestClientV2Wrapper client2 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);

    // Then
    assertThat(client1)
        .as("Should return the same client instance for same pipe name")
        .isEqualTo(client2);
  }

  @Test
  public void testGetClient_DifferentPipeNames_CreatesDistinctClients() {
    // When
    StreamingIngestClientV2Wrapper client1 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);
    StreamingIngestClientV2Wrapper client2 =
        provider.getClient(connectorConfig, testPipeName2, streamingClientProperties);

    // Then
    assertThat(client1)
        .as("Different pipe names should create different clients")
        .isNotEqualTo(client2);
  }

  @Test
  public void testGetClient_AfterClientClosed_CreatesNewClient() {
    // Given
    StreamingIngestClientV2Wrapper client1 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);
    // Close the client through the provider
    provider.close(testPipeName);

    // When
    StreamingIngestClientV2Wrapper client2 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);

    // Then
    assertThat(client1)
        .as("Should create a new client when previous is closed")
        .isNotEqualTo(client2);
  }

  @Test
  public void testClose_ExistingPipe_ClosesAndRemovesClient() {
    // Given
    StreamingIngestClientV2Wrapper client =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);

    // When
    provider.close(testPipeName);

    // Then - Verify new client is created for same pipe name
    StreamingIngestClientV2Wrapper newClient =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);
    assertThat(client).as("Should create new client after close").isNotEqualTo(newClient);
  }

  @Test
  public void testClose_NonExistentPipe_DoesNotThrow() {
    // When/Then - Should not throw exception
    assertThatCode(() -> provider.close("non_existent_pipe")).doesNotThrowAnyException();
  }

  @Test
  public void testCloseAll_MultipleClients_ClosesAllClients() {
    // Given
    StreamingIngestClientV2Wrapper client1 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);
    StreamingIngestClientV2Wrapper client2 =
        provider.getClient(connectorConfig, testPipeName2, streamingClientProperties);

    // When
    provider.closeAll();

    // Then - Verify new clients are created after closeAll
    StreamingIngestClientV2Wrapper newClient1 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);
    StreamingIngestClientV2Wrapper newClient2 =
        provider.getClient(connectorConfig, testPipeName2, streamingClientProperties);

    assertThat(client1).as("Should create new client after closeAll").isNotEqualTo(newClient1);
    assertThat(client2).as("Should create new client after closeAll").isNotEqualTo(newClient2);
  }

  @Test
  public void testCloseAll_EmptyProvider_DoesNotThrow() {
    // When/Then - Should not throw exception
    assertThatCode(() -> provider.closeAll()).doesNotThrowAnyException();
  }

  @Test
  public void testProvider_ReuseAfterPartialClose_WorksCorrectly() {
    // Given - Multiple clients
    StreamingIngestClientV2Wrapper client1 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);
    StreamingIngestClientV2Wrapper client2 =
        provider.getClient(connectorConfig, testPipeName2, streamingClientProperties);

    // When - Close only one client
    provider.close(testPipeName);

    // Then - New client for first pipe should be created
    StreamingIngestClientV2Wrapper newClient1 =
        provider.getClient(connectorConfig, testPipeName, streamingClientProperties);
    assertThat(client1).as("Should create new client for closed pipe").isNotEqualTo(newClient1);

    // Existing client for second pipe should be reused
    StreamingIngestClientV2Wrapper sameClient2 =
        provider.getClient(connectorConfig, testPipeName2, streamingClientProperties);
    assertThat(client2).as("Should reuse existing client for open pipe").isEqualTo(sameClient2);
  }
}
