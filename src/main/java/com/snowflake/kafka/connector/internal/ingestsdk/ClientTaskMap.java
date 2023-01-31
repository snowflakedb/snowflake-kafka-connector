package com.snowflake.kafka.connector.internal.ingestsdk;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
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
    private final int taskCount;
    private final int clientCount;

    // don't open or close client here, this just tracks state
    public ClientTaskMap(int taskCount, int numTasksPerClient)  {
        this.taskCount = taskCount;
        // round up number of clients
        this.clientCount = (int) Math.ceil(taskCount / numTasksPerClient);

        LOGGER = new LoggerHandler(this.getClass().getName());
        this.clientTaskMap = new HashMap<>();
        this.areAllClientsInitialized = false;

        int taskIdx = 0;
        int clientIdx = 0;

        while (clientIdx < this.clientCount) {
            List<Integer> taskIdList = new ArrayList<>();

            while (taskIdx < this.taskCount) {
                taskIdList.add(taskIdx);
                taskIdx++;
            }

            this.clientTaskMap.put(taskIdList, null);
            clientIdx++;
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

                if (!this.isClientValid(client)) {
                    throw SnowflakeErrors.ERROR_3009.getException();
                }

                return client;
            }
        }

        throw SnowflakeErrors.ERROR_3010.getException();
    }

    public void addClient(List<Integer> taskList, SnowflakeStreamingIngestClient client) {
        if (!this.isClientValid(client)) {
            throw SnowflakeErrors.ERROR_3009.getException();
        }

        SnowflakeStreamingIngestClient prevClient = this.clientTaskMap.get(taskList);
        if (this.isClientValid(prevClient)) {
            LOGGER.warn("Replacing previous valid client instance '{}' with new client instance '{}'", prevClient.getName(), client.getName());
            this.removeClient(prevClient);
        }

        this.clientTaskMap.put(taskList, client);
    }

    public void removeClient(SnowflakeStreamingIngestClient client) {
        for (Map.Entry<List<Integer>, SnowflakeStreamingIngestClient> entry : this.clientTaskMap.entrySet()) {
            SnowflakeStreamingIngestClient currClient = entry.getValue();

            if (currClient != null && currClient.getName().equals(client.getName())) {
                this.clientTaskMap.remove(entry.getKey());
                return;
            }
        }

        LOGGER.warn("Given client could not be removed because it was not found");
    }

    // validate consecutive tasks and all clients are initialized
    public List<String> validateMap(int initializedClientCount) {
        List<String> exceptionMsgs = new ArrayList<>();
        boolean[] taskIds = new boolean[this.taskCount];
        List<SnowflakeStreamingIngestClient> invalidClients = new ArrayList<>();

        for (Map.Entry<List<Integer>, SnowflakeStreamingIngestClient> entry : this.clientTaskMap.entrySet()) {
            List<Integer> currTaskList = entry.getKey();
            SnowflakeStreamingIngestClient currClient = entry.getValue();

            // get existing task ids
            for (int taskIdx : currTaskList) {
                if (taskIdx < taskIds.length && taskIdx >= 0) {
                    taskIds[taskIdx] = true;
                } else {
                    exceptionMsgs.add(Utils.formatString("Mapped taskid {} was out of bounds, expected a number between {} - {}", taskIdx, 0, this.taskCount));
                }
            }

            // get invalid clients
            if (!this.isClientValid(currClient)) {
                invalidClients.add(currClient);
            }
        }

        // check expected client and actual client counts are the same
        if (this.clientTaskMap.size() != initializedClientCount) {
            exceptionMsgs.add(Utils.formatString("Expected {} initialized clients, but actually have {} initialized clients", this.clientCount, initializedClientCount));
        }

        // if any clients were invalid
        if (!invalidClients.isEmpty()) {
            String invalidClientsStr = invalidClients.get(0).getName();
            for (int invalidClientIdx = 1; invalidClientIdx < invalidClients.size(); invalidClientIdx++) {
                invalidClientsStr += ", " + invalidClients.get(invalidClientIdx).getName();
            }
            exceptionMsgs.add(Utils.formatString("There were {} invalid clients: {}", invalidClients.size(), invalidClientsStr));
        }

        String invalidTaskIds = "";
        for (int taskIdx = 0; taskIdx < taskIds.length; taskIdx++) {
            if (!taskIds[taskIdx]) {
                invalidTaskIds += taskIdx + ", ";
            }
        }
        if (!invalidTaskIds.isEmpty()) {
            exceptionMsgs.add(Utils.formatString("Not enough tasks were mapped to clients, tasks missing clients: {}", invalidTaskIds.substring(0, invalidTaskIds.lastIndexOf(','))));
        }

        return exceptionMsgs;
    }

    private boolean isClientValid(SnowflakeStreamingIngestClient client) {
        return client != null && !client.isClosed();
    }
}
