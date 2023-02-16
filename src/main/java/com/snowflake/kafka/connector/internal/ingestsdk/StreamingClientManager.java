/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.internal.ingestsdk;

import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Provides access to the streaming ingest clients. This should be the only place to manage clients.
 */
public class StreamingClientManager {
  private LoggerHandler LOGGER;

  private Map<Integer, KcStreamingIngestClient> taskToClientMap;
  private int maxTasks;
  private final int minTasks = 0;
  private int clientId; // this should only ever increase

  // TESTING ONLY - inject the client map
  @VisibleForTesting
  public StreamingClientManager(Map<Integer, KcStreamingIngestClient> taskToClientMap) {
    this();
    this.taskToClientMap = taskToClientMap;
    this.clientId = (int) taskToClientMap.values().stream().distinct().count() - 1;
  }

  /** Creates a new client manager */
  protected StreamingClientManager() {
    LOGGER = new LoggerHandler(this.getClass().getName());
    this.taskToClientMap = new HashMap<>();
    this.maxTasks = 0;
    this.clientId = -1; // will be incremented when a client is created
  }

  /** Gets the task to client map associated with StreamingClientManager */
  @VisibleForTesting
  public Map<Integer, KcStreamingIngestClient> getTaskToClientMap() {
    return taskToClientMap;
  }

  /**
   * Creates as many clients as needed with the connector config and kc instance id. This assumes
   * that all taskIds are consecutive ranging from 0 to maxTasks.
   *
   * @param connectorConfig the config for the clients, cannot be null
   * @param kcInstanceId the kafka connector id requesting the clients, cannot be null
   * @param maxTasks the max number of tasks assigned to this connector, must be greater than 0
   * @param numTasksPerClient the max number of tasks to be assigned to each client, must be greater
   *     than 0
   */
  public void createAllStreamingClients(
      Map<String, String> connectorConfig,
      String kcInstanceId,
      int maxTasks,
      int numTasksPerClient) {
    assert connectorConfig != null && kcInstanceId != null && maxTasks > 0 && numTasksPerClient > 0;

    this.maxTasks = maxTasks;

    int clientCount = (int) Math.ceil((double) maxTasks / (double) numTasksPerClient);

    Properties clientProperties = new Properties();
    clientProperties.putAll(
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(connectorConfig)));

    // Override only if bdec version is explicitly set in config, default to the version set inside
    // Ingest SDK
    Map<String, Object> parameterOverrides = new HashMap<>();
    Optional<String> snowpipeStreamingBdecVersion =
        Optional.ofNullable(
            connectorConfig.get(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION));

    snowpipeStreamingBdecVersion.ifPresent(
        overriddenValue -> parameterOverrides.put(BLOB_FORMAT_VERSION, overriddenValue));

    LOGGER.info(
        "Creating {} clients for {} tasks with max {} tasks per client using {} file format",
        clientCount,
        maxTasks,
        numTasksPerClient,
        snowpipeStreamingBdecVersion);

    // put a new client for every tasksToCurrClient taskIds
    int tasksToCurrClient = 0;
    KcStreamingIngestClient createdClient =
        this.getClientHelper(
            clientProperties,
            parameterOverrides,
            kcInstanceId,
            0); // asserted that we have at least 1 task

    for (int taskId = 0; taskId < this.maxTasks; taskId++) {
      if (tasksToCurrClient == numTasksPerClient) {
        createdClient =
            this.getClientHelper(clientProperties, parameterOverrides, kcInstanceId, taskId);
        tasksToCurrClient = 1;
      } else {
        tasksToCurrClient++;
      }

      this.taskToClientMap.put(taskId, createdClient);
    }
  }

  // builds the client name and returns the created client. note taskId is used just for logging
  private KcStreamingIngestClient getClientHelper(
      Properties props, Map<String, Object> parameterOverrides, String kcInstanceId, int taskId) {
    this.clientId++;
    String clientName =
        KcStreamingIngestClient.buildStreamingIngestClientName(kcInstanceId, this.clientId);
    LOGGER.debug("Creating client {} for taskid {}", clientName, taskId);

    return new KcStreamingIngestClient(props, parameterOverrides, clientName);
  }

  /**
   * Gets the client corresponding to the task id and validates it (not null and is closed)
   *
   * @param taskId the task id to get the corresponding client
   * @return The streaming client, throws an exception if no client was initialized
   */
  public KcStreamingIngestClient getValidClient(int taskId) {
    if (taskId > this.maxTasks || taskId < this.minTasks) {
      throw SnowflakeErrors.ERROR_3010.getException(
          Utils.formatString(
              "taskId must be between 0 and {} but was given {}", this.maxTasks, taskId));
    }

    if (this.clientId < 0) {
      throw SnowflakeErrors.ERROR_3009.getException("call the manager to create the clients");
    }

    KcStreamingIngestClient client = this.taskToClientMap.get(taskId);
    if (client == null || client.isClosed()) {
      throw SnowflakeErrors.ERROR_3009.getException();
    }

    return client;
  }

  /**
   * Gets the number of clients created
   *
   * @return the number of clients created
   */
  public int getClientCount() {
    return this.clientId + 1; // clientid starts at 0, so off by one
  }

  /**
   * Closes all the streaming clients in the map. Client closure exceptions will be swallowed and
   * logged
   *
   * @return if all the clients were closed
   */
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
