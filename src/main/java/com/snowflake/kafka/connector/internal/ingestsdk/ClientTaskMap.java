package com.snowflake.kafka.connector.internal.ingestsdk;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClientTaskMap {
    private LoggerHandler LOGGER;

    private Map<List<Integer>, SnowflakeStreamingIngestClient> clientTaskMap;
    private boolean areAllClientsInitialized;
    private final int taskCount;
    private final int clientCount;

    // don't open or close client here, this just manages clients we are using
    // map is initialized with invalid clients, they must be initialized and added later
    // this assumes that the taskIds start from 0 and are consecutive. Keep in mind all tasks may not be used
    public ClientTaskMap(int taskCount, int numTasksPerClient)  {
        LOGGER = new LoggerHandler(this.getClass().getName());
        this.clientTaskMap = new HashMap<>();
        this.areAllClientsInitialized = false;

        if (taskCount <= 0 || numTasksPerClient <= 0) {
            throw SnowflakeErrors.ERROR_3011.getException(Utils.formatString("taskCount: {}, numTasksPerClient: {}", taskCount, numTasksPerClient));
        }

        this.taskCount = taskCount;
        // round up number of clients
        this.clientCount = numTasksPerClient;

        int taskIdx = 0;
        int clientIdx = 0;
        List<Integer> taskIdList = new ArrayList<>();

        while (taskIdx < this.taskCount) {
            if (clientIdx == this.clientCount) {
                this.clientTaskMap.put(taskIdList, null);
                taskIdList = new ArrayList<>();
                clientIdx = 0;
            }

            taskIdList.add(taskIdx);
            clientIdx++;
            taskIdx++;
        }

        if (!taskIdList.isEmpty()) {
            this.clientTaskMap.put(taskIdList, null);
        }
    }

    // taskids could correspond to invalid clients, dont modify state
    public Set<List<Integer>> getTaskIdLists() {
        return new HashSet<>(this.clientTaskMap.keySet());
    }

    // returns only valid clients, dont modify state
    public Set<SnowflakeStreamingIngestClient> getValidClients() {
        if (!this.areAllClientsInitialized) {
            LOGGER.warn("Not all streaming ingest clients were initialized");
        }

        // ignore invalid clients
        Collection<SnowflakeStreamingIngestClient> clients =  this.clientTaskMap.values();
        clients.removeIf(client -> !this.isClientValid(client));
        return new HashSet<>(clients);
    }

    // returns only valid clients, dont modify state, throws if invalid or not found
    public SnowflakeStreamingIngestClient getValidClient(int taskId) {
        for (List<Integer> taskList : this.clientTaskMap.keySet()) {
            if (taskList.contains(taskId)) {
                SnowflakeStreamingIngestClient client = this.clientTaskMap.get(taskList);

                if (!this.isClientValid(client)) {
                    throw SnowflakeErrors.ERROR_3009.getException("Existing client in map is not valid");
                }

                return client;
            }
        }

        throw SnowflakeErrors.ERROR_3010.getException(Utils.formatString("Could not find client mapped to taskid: {}", taskId));
    }

    // upserts valid clients, errors if entry doesnt exist already
    public void upsertClient(List<Integer> taskList, SnowflakeStreamingIngestClient client) {
        if (!this.isClientValid(client)) {
            throw SnowflakeErrors.ERROR_3009.getException();
        }

        if (!this.clientTaskMap.keySet().contains(taskList)) {
            throw SnowflakeErrors.ERROR_3010.getException("Could not find task id list in existing map");
        }

        SnowflakeStreamingIngestClient prevClient = this.clientTaskMap.get(taskList);
        if (this.isClientValid(prevClient)) {
            LOGGER.warn("Replacing previous valid client instance '{}' with new client instance '{}'", prevClient.getName(), client.getName());
            this.removeClientTaskEntry(prevClient);
        }

        this.clientTaskMap.put(taskList, client);
    }

    // removes all instances of the client, ignores if client doesnt exist
    public void removeClientTaskEntry(SnowflakeStreamingIngestClient client) {
        String clientName = client.getName();
        Map<List<Integer>, SnowflakeStreamingIngestClient> newClientTaskMap = new HashMap<>();

        for (Map.Entry<List<Integer>, SnowflakeStreamingIngestClient> entry : this.clientTaskMap.entrySet()) {
            SnowflakeStreamingIngestClient currClient = entry.getValue();

            if (!this.areClientsEqual(currClient, client)) {
                newClientTaskMap.put(entry.getKey(), currClient);
            } else {
                // dont add to new map
                LOGGER.debug("Removing client {}", clientName);
            }
        }

        if (newClientTaskMap.size() == this.clientTaskMap.size()) {
            LOGGER.warn("Given client {} could not be removed because it was not found", clientName);
        } else {
            this.clientTaskMap = newClientTaskMap;
        }
    }

    // validate taskids are consecutive and that all clients are unique and initialized
    public String validateMap(int initializedClientCount) {
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

        // correct number of tasks
        String invalidTaskIds = "";
        for (int taskIdx = 0; taskIdx < taskIds.length; taskIdx++) {
            if (!taskIds[taskIdx]) {
                invalidTaskIds += taskIdx + ", ";
            }
        }
        if (!invalidTaskIds.isEmpty()) {
            exceptionMsgs.add(Utils.formatString("Not enough tasks were mapped to clients, tasks missing clients: {}", invalidTaskIds.substring(0, invalidTaskIds.lastIndexOf(','))));
        }

        return exceptionMsgs.toString();
    }

    private boolean isClientValid(SnowflakeStreamingIngestClient client) {
        return client != null && !client.isClosed();
    }

    private boolean areClientsEqual(SnowflakeStreamingIngestClient client1, SnowflakeStreamingIngestClient client2) {
        if (client1 == null) {
            return client2 == null;
        }

        return (client1.isClosed() == client2.isClosed())
                && (client1.getName().equals(client2.getName()));
    }
}
