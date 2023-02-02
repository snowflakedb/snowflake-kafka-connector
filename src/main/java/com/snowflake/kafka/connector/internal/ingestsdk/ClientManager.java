package com.snowflake.kafka.connector.internal.ingestsdk;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// provides access to clients
public class ClientManager {
  private LoggerHandler LOGGER;

  private Map<Integer, KcStreamingIngestClient> taskToClientMap;

  // TESTING ONLY - inject the client map
  @VisibleForTesting
  public ClientManager(Map<Integer, KcStreamingIngestClient> taskToClientMap) {
    this();
    this.taskToClientMap = taskToClientMap;
  }

  protected ClientManager() {
    LOGGER = new LoggerHandler(this.getClass().getName());
    this.taskToClientMap = new HashMap<>();
  }

  // note: assumes taskids are consecutive and starts from 0
  public void createAllStreamingClients(
      Map<String, String> connectorConfig,
      String kcInstanceId,
      int maxTasks,
      int numTasksPerClient) {
    assert connectorConfig != null;
    assert kcInstanceId != null;
    assert maxTasks > 0;
    assert numTasksPerClient > 0;

    int clientCount = (int) Math.ceil((double) maxTasks / (double) numTasksPerClient);
    LOGGER.info(
        "Creating {} clients for {} tasks with max {} tasks per client",
        clientCount,
        maxTasks,
        numTasksPerClient);

    Properties clientProperties = new Properties();
    clientProperties.putAll(
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(connectorConfig)));

    int taskId = 0;
    int clientId = 0;
    int tasksToCurrClient = 0;
    KcStreamingIngestClient createdClient =
        new KcStreamingIngestClient(
            clientProperties,
            KcStreamingIngestClient.buildStreamingIngestClientName(kcInstanceId, clientId));
    while (taskId < maxTasks) {
      if (tasksToCurrClient == numTasksPerClient) {
        createdClient =
            new KcStreamingIngestClient(
                clientProperties,
                KcStreamingIngestClient.buildStreamingIngestClientName(kcInstanceId, clientId));
        this.taskToClientMap.put(taskId, createdClient);
        tasksToCurrClient = 1;
        clientId++;
      } else {
        this.taskToClientMap.put(taskId, createdClient);
        tasksToCurrClient++;
      }

      taskId++;
    }
  }

  /**
   * Gets the streaming client if all clients are valid note: could return just the valid client if
   * the others were not setup
   *
   * @return The streaming client, throws an exception if no client was initialized
   */
  public KcStreamingIngestClient getValidClient(int taskId) {
    KcStreamingIngestClient client = this.taskToClientMap.get(taskId);
    if (client == null || client.isClosed()) {
      throw SnowflakeErrors.ERROR_3009.getException();
    }

    return client;
  }

  public boolean closeAllStreamingClients() {
    boolean isAllClosed = true;
    LOGGER.info("Closing all clients");

    for (Integer taskId : this.taskToClientMap.keySet()) {
      KcStreamingIngestClient client = this.taskToClientMap.get(taskId);
      isAllClosed &= client.close();
    }

    return isAllClosed;
  }
}
