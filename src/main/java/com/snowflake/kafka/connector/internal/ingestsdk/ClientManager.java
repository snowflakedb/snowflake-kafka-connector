package com.snowflake.kafka.connector.internal.ingestsdk;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** This is a wrapper to help manage the streaming ingest clients */
public class ClientManager {
  private LoggerHandler LOGGER;

  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";

  private int streamingIngestClientCount;

  private ClientTaskMap clientTaskMap;

  protected ClientManager() {
    LOGGER = new LoggerHandler(this.getClass().getName());
    this.streamingIngestClientCount = 0;
  }

  // ONLY FOR TESTING - use this to inject client map
  @VisibleForTesting
  public ClientManager(ClientTaskMap clientTaskMap) {
    this();
    this.clientTaskMap = clientTaskMap;
  }

  public void createAllStreamingClients(Map<String, String> connectorConfig, String kcInstanceId, ClientTaskMap clientTaskMap) {
    for (List<Integer> taskList : clientTaskMap.getTaskIdLists()) {
      SnowflakeStreamingIngestClient client = this.createStreamingClient(connectorConfig, kcInstanceId);
      clientTaskMap.addClient(taskList, client);
      this.streamingIngestClientCount++;
    }

    this.clientTaskMap.validateMap(this.streamingIngestClientCount);
    this.clientTaskMap = clientTaskMap;
  }

  /**
   * Gets the streaming client if it was created
   *
   * @return The streaming client, throws an exception if no client was initialized
   */
  public SnowflakeStreamingIngestClient getStreamingIngestClient(int taskId) {
    return this.clientTaskMap.getClient(taskId);
  }

  public void closeAllStreamingClients() {
    for (SnowflakeStreamingIngestClient client : this.clientTaskMap.getClients()) {
      this.clientTaskMap.removeClient(client);
      this.streamingIngestClientCount--;
    }

    this.clientTaskMap.validateMap(this.streamingIngestClientCount);
    this.clientTaskMap = null;
  }

  /**
   * Calls the ingest sdk to create the streaming client, retries on exception
   *
   * @param connectorConfig properties for the streaming client
   * @param kcInstanceId identifier of the connector instance creating the client
   */
  private SnowflakeStreamingIngestClient createStreamingClient(Map<String, String> connectorConfig, String kcInstanceId) {
    Map<String, String> streamingPropertiesMap =
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(connectorConfig));
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(streamingPropertiesMap);

    String streamingIngestClientName = this.getStreamingIngestClientName(kcInstanceId);
      try {
        LOGGER.info("Creating Streaming Client. ClientName:{}", streamingIngestClientName);
        return SnowflakeStreamingIngestClientFactory.builder(streamingIngestClientName)
                        .setProperties(streamingClientProps)
                        .build();
      } catch (SFException ex) {
          throw new ConnectException(ex);
      }
  }

  /**
   * Calls the ingest sdk to close the client sdk, retries on failure
   */
  private boolean closeStreamingClient(SnowflakeStreamingIngestClient streamingIngestClient) {
    if (streamingIngestClient == null || streamingIngestClient.isClosed()) {
      LOGGER.info("Streaming client is already closed or null");
      return true;
    }

    String streamingIngestClientName = streamingIngestClient.getName();
    LOGGER.info("Closing Streaming Client:{}", streamingIngestClientName);

      try {
        streamingIngestClient.close();
        // TODO: add client count and map verification here
        return true;
      } catch (Exception e) {
        String message =
                e.getMessage() != null && !e.getMessage().isEmpty()
                        ? e.getMessage()
                        : "no error message provided";

        String cause =
                e.getCause() != null
                        && e.getCause().getStackTrace() != null
                        && !Arrays.toString(e.getCause().getStackTrace()).isEmpty()
                        ? Arrays.toString(e.getCause().getStackTrace())
                        : "no cause provided";
        // don't throw an exception because closing the client here is best effort
        LOGGER.error("Failure closing Streaming client msg:{}, cause:{}", message, cause);
    }

    return false;
  }

  /**
   * Gets the clients name by adding a prefix and client count
   *
   * @param kcInstanceId the indentifier for the connector creating this client
   * @return the streaming ingest client name
   */
  private String getStreamingIngestClientName(String kcInstanceId) {
    return STREAMING_CLIENT_PREFIX_NAME + kcInstanceId + this.streamingIngestClientCount;
  }
}
