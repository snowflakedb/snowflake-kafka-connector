package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.Constants.DEFAULT_PIPE_NAME_SUFFIX;
import static org.assertj.core.api.Assertions.*;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
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

public class StreamingClientManagerIT {

  private Map<String, String> connectorConfig;
  private StreamingClientProperties streamingClientProperties;
  private String testConnectorName;
  private String pipe1, pipe2;
  private String task1, task2;
  private String table1, table2;

  @BeforeEach
  public void setUp() {
    final long salt = System.currentTimeMillis();
    final SnowflakeConnectionService connectionService =
        TestUtils.getConnectionServiceForStreaming();
    connectorConfig = TestUtils.getConfForStreaming();
    streamingClientProperties = new StreamingClientProperties(connectorConfig);
    table1 = "table1" + salt;
    table2 = "table2" + salt;
    task1 = "task1" + salt;
    task2 = "task2" + salt;
    testConnectorName = "TEST_CONNECTOR_" + salt;
    pipe1 = table1 + DEFAULT_PIPE_NAME_SUFFIX;
    pipe2 = table2 + DEFAULT_PIPE_NAME_SUFFIX;
    connectionService.createTable(table1);
    connectionService.createTable(table2);
  }

  @AfterEach
  public void tearDown() {
    TestUtils.dropTable(table1);
    TestUtils.dropTable(table2);
    closeTaskClients(task1);
    closeTaskClients(task2);
  }

  @Test
  public void testGetClient_FirstTime_CreatesNewClient() {
    // When
    SnowflakeStreamingIngestClient client = getClient(task1, pipe1);
    // Then
    assertThat(client).as("Client should not be null").isNotNull();
  }

  @Test
  public void testGetClient_SamePipeName_ReturnsExistingClient() {
    // Given
    SnowflakeStreamingIngestClient client1 = getClient(task1, pipe1);

    // When
    SnowflakeStreamingIngestClient client2 = getClient(task1, pipe1);

    // Then
    assertThat(client1)
        .as("Should return the same client instance for same pipe name")
        .isEqualTo(client2);
  }

  @Test
  public void testGetClient_DifferentPipeNames_CreatesDistinctClients() {
    // When
    SnowflakeStreamingIngestClient client1 = getClient(task1, pipe1);
    SnowflakeStreamingIngestClient client2 = getClient(task2, pipe2);
    // Then
    assertThat(client1)
        .as("Different pipe names should create different clients")
        .isNotEqualTo(client2);
  }

  @Test
  public void testGetClient_AfterClientClosed_CreatesNewClient() {
    // Given
    SnowflakeStreamingIngestClient client1 = getClient(task1, pipe1);
    // Close the client for this task
    closeTaskClients(task1);

    // When
    SnowflakeStreamingIngestClient client2 = getClient(task1, pipe1);

    // Then
    assertThat(client1)
        .as("Should create a new client when previous task released it")
        .isNotEqualTo(client2);
  }

  @Test
  public void testClose_ExistingPipe_ClosesAndRemovesClient() {
    // Given
    SnowflakeStreamingIngestClient client = getClient(task1, pipe1);

    // When - Release the task
    closeTaskClients(task1);

    // Then - Verify new client is created for same pipe name with different task
    SnowflakeStreamingIngestClient newClient = getClient(task2, pipe1);
    assertThat(client).as("Should create new client after close").isNotEqualTo(newClient);
  }

  @Test
  public void testClose_NonExistentPipe_DoesNotThrow() {
    assertThatCode(() -> closeTaskClients("nonExistentTask")).doesNotThrowAnyException();
  }

  @Test
  public void testClose_MultipleClients_ClosesAllClients() {
    // Given
    getClient(task1, pipe1);
    getClient(task1, pipe2);
    assertThat(StreamingClientManager.getClientCountForTask(testConnectorName, task1)).isEqualTo(2);
    closeTaskClients(task1);
    assertThat(StreamingClientManager.getClientCountForTask(testConnectorName, task1)).isEqualTo(0);
  }

  @Test
  void test_GetClientProperties_includes_role() {
    // Given
    Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put(Utils.SF_URL, "https://test.snowflakecomputing.com");
    connectorConfig.put(Utils.SF_PRIVATE_KEY, "test_private_key");
    connectorConfig.put(Utils.SF_USER, "test_user");
    connectorConfig.put(Utils.SF_ROLE, "TEST_ROLE");

    // When
    Properties properties = StreamingClientManager.getClientProperties(connectorConfig);

    // Then
    assertThat(properties).isNotNull();
    assertThat(properties.getProperty("role")).isEqualTo("TEST_ROLE");
    assertThat(properties.getProperty("user")).isEqualTo("test_user");
    assertThat(properties.getProperty("private_key")).isEqualTo("test_private_key");
  }

  @Test
  public void testProvider_ReuseAfterPartialClose_WorksCorrectly() {
    // task 1 uses 2 pipes, so it has 2 ingest clients
    SnowflakeStreamingIngestClient client1 = getClient(task1, pipe1);
    SnowflakeStreamingIngestClient client2 = getClient(task1, pipe2);

    // Task2 also uses pipe1 (shares one client with task1) in total there should only be 2 ingest
    // clients in the system
    SnowflakeStreamingIngestClient client3 = getClient(task2, pipe1);

    assertThat(client1).isEqualTo(client3);

    // When - task1 stops
    closeTaskClients(task1);

    // Then - Client1 should still be open (task2 still using it)
    SnowflakeStreamingIngestClient client1AfterRelease = getClient(task1, pipe1);

    //  should get the SAME client1 that task2 is still using
    assertThat(client1AfterRelease)
        .as("Should reuse client when another task is still using it")
        .isEqualTo(client1);
  }

  private SnowflakeStreamingIngestClient getClient(String task, String pipe) {
    return StreamingClientManager.getClient(
        testConnectorName, task, pipe, connectorConfig, streamingClientProperties);
  }

  private void closeTaskClients(String task) {
    StreamingClientManager.closeTaskClients(testConnectorName, task);
  }
}
