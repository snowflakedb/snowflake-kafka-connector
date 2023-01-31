package com.snowflake.kafka.connector.internal.ingestsdk;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClientTaskMap {
    private LoggerHandler LOGGER;

    private Map<List<Integer>, SnowflakeStreamingIngestClient> clientTaskMap;
    private boolean areAllClientsInitialized;

    public ClientTaskMap(int taskCount, int clientCount)  {
        LOGGER = new LoggerHandler(this.getClass().getName());
        this.clientTaskMap = new HashMap<>();
        this.areAllClientsInitialized = false;

        int clientIdx = 0;
        int taskIdx = 0;
        List<Integer> taskIdList = new ArrayList<>();

        while (taskIdx < taskCount) {
            taskIdList.add(taskIdx);
            taskIdx++;
            clientIdx++;

            if (clientIdx == clientCount) {
                // initialize map with null clients
                this.clientTaskMap.put(taskIdList, null);

                taskIdList = new ArrayList<>();
                clientIdx = 0;
            }
        }
    }

    public Set<List<Integer>> getTaskIdLists() {
        return clientTaskMap.keySet();
    }

    public Set<SnowflakeStreamingIngestClient> getClients() {
        if (!this.areAllClientsInitialized) {
            LOGGER.warn("Not all streaming ingest clients were initialized");
        }

        return (Set<SnowflakeStreamingIngestClient>) this.clientTaskMap.values();
    }

    public SnowflakeStreamingIngestClient getClient(int taskId) {
        for (List<Integer> taskList : this.clientTaskMap.keySet()) {
            if (taskList.contains(taskId)) {
                SnowflakeStreamingIngestClient client = this.clientTaskMap.get(taskList);

                if (client == null || client.isClosed()) {
                    LOGGER.error("Streaming ingest client was null or closed. It must be initialized");
                    throw SnowflakeErrors.ERROR_3009.getException();
                }

                return client;
            }
        }
    }

    public void addClient(List<Integer> taskList, SnowflakeStreamingIngestClient client) {

    }

    public void removeClient(SnowflakeStreamingIngestClient client) {

    }

    public void validateMap(int streamingIngestClientCount) {
        this.areAllClientsInitialized

    }
}
