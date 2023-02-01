package com.snowflake.kafka.connector.internal.ingestsdk;

import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.utils.SFException;

// provides access to clients
public class ClientManager {
  private LoggerHandler LOGGER;

  private Map<Integer, KcStreamingIngestClient> taskToClientMap;
  private boolean areAllClientsInitialized;
  private int clientCount;
  private int taskCount;

  protected ClientManager() {
    LOGGER = new LoggerHandler(this.getClass().getName());
    this.taskToClientMap = new HashMap<>();
    this.areAllClientsInitialized = false;
    this.clientCount = 0;
    this.taskCount = 0;
  }

  // note: assumes taskids are consecutive and starts from 0
  public void createAllStreamingClients(
      Map<String, String> connectorConfig,
      String kcInstanceId,
      int maxTasks,
      int numTasksPerClient) {
    this.areAllClientsInitialized = false;

    assert connectorConfig != null;
    assert kcInstanceId != null;
    assert maxTasks > 0;
    assert numTasksPerClient > 0;
    // TODO @rcheng: assert handling?

    this.taskCount = maxTasks;
    this.clientCount = (int) Math.ceil((double) maxTasks / (double) numTasksPerClient);
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

    this.areAllClientsInitialized = true;
  }

  /**
   * Gets the streaming client if all clients are valid note: could return just the valid client if
   * the others were not setup
   *
   * @return The streaming client, throws an exception if no client was initialized
   */
  public KcStreamingIngestClient getStreamingIngestClient(int taskId) {
    if (this.taskCount > taskId) {
      throw new SFException(null);
      // TODO @rcheng: error handling?
    }

    if (!this.areAllClientsInitialized) {
      throw new SFException(null);
    }

    return this.taskToClientMap.get(taskId);
  }

  public boolean closeAllStreamingClients() {
    boolean isAllClosed = true;
    LOGGER.info("Closing all {} clients...", clientCount);

    for (Integer taskId : this.taskToClientMap.keySet()) {
      KcStreamingIngestClient client = this.taskToClientMap.get(taskId);
      isAllClosed &= client.close();
      this.areAllClientsInitialized = false;
    }

    return isAllClosed;
  }
}
