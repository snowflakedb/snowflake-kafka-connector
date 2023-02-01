///*
// * Copyright (c) 2023 Snowflake Inc. All rights reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.snowflake.kafka.connector.internal.ingestsdk;
//
//import com.snowflake.kafka.connector.internal.SnowflakeErrors;
//import com.snowflake.kafka.connector.internal.TestUtils;
//import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mockito;
//
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//public class ClientTaskMapTest {
//    @Before
//    // set up sunny day tests
//    public void setup() {
//    }
//
//    @Test
//    public void testConstructorAndGetsTaskLists() {
//        // create a [taskId] -> client map
//        // 5 tasks, 2 tasks per client = [0, 1] -> clientA, [2, 3] -> clientB, [4] -> clientC
//        Set<List<Integer>> goalTaskIdLists = new HashSet<>();
//        goalTaskIdLists.add(Arrays.asList(0, 1));
//        goalTaskIdLists.add(Arrays.asList(2, 3));
//        goalTaskIdLists.add(Arrays.asList(4));
//        this.testConstructorAndGetTaskIdListsRunner(5, 2, goalTaskIdLists);
//
//        // 4 tasks, 2 tasks per client = [0, 1] -> clientA, [2, 3] -> clientB
//        goalTaskIdLists = new HashSet<>();
//        goalTaskIdLists.add(Arrays.asList(0, 1));
//        goalTaskIdLists.add(Arrays.asList(2, 3));
//        this.testConstructorAndGetTaskIdListsRunner(4, 2, goalTaskIdLists);
//
//        // 6 tasks, 5 tasks per client = [0, 1, 2, 3, 4] -> clientA, [5] -> clientB
//        goalTaskIdLists = new HashSet<>();
//        goalTaskIdLists.add(Arrays.asList(0, 1, 2, 3, 4));
//        goalTaskIdLists.add(Arrays.asList(5));
//        this.testConstructorAndGetTaskIdListsRunner(6, 5, goalTaskIdLists);
//    }
//
//    private void testConstructorAndGetTaskIdListsRunner(int taskCount, int numTasksPerClient, Set<List<Integer>> goalTaskIdLists) {
//        // test creation and get task ids
//        ClientManager clientManager = new ClientManager(taskCount, numTasksPerClient);
//        Set<List<Integer>> gotTaskIdLists = clientManager.getTaskIdLists();
//
//        // verify taskid lists are as expected
//        assert gotTaskIdLists.size() == goalTaskIdLists.size();
//        for (List<Integer> goalTaskIdList : goalTaskIdLists) {
//            assert gotTaskIdLists.contains(goalTaskIdList);
//        }
//    }
//
//    @Test
//    public void testConstructorInvalidParams() {
//        assert TestUtils.assertError(SnowflakeErrors.ERROR_3011,
//                () -> new ClientManager(0, 100));
//
//        assert TestUtils.assertError(SnowflakeErrors.ERROR_3011,
//                () -> new ClientManager(0, 0));
//
//        assert TestUtils.assertError(SnowflakeErrors.ERROR_3011,
//                () -> new ClientManager(100, 0));
//
//        assert TestUtils.assertError(SnowflakeErrors.ERROR_3011,
//                () -> new ClientManager(-100, 100));
//
//        assert TestUtils.assertError(SnowflakeErrors.ERROR_3011,
//                () -> new ClientManager(-100, -100));
//
//        assert TestUtils.assertError(SnowflakeErrors.ERROR_3011,
//                () -> new ClientManager(100, -100));
//    }
//
//    @Test
//    public void testGetAndAddClients() {
//        // 5 tasks, 2 tasks per client = [0, 1] -> clientA, [2, 3] -> clientB, [4] -> clientC
//        // setup
//        List<Integer> clientATaskList = Arrays.asList(0, 1);
//        List<Integer> clientBTaskList = Arrays.asList(2, 3);
//        List<Integer> clientCTaskList = Arrays.asList(4);
//
//        SnowflakeStreamingIngestClient clientA = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        SnowflakeStreamingIngestClient clientB = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        SnowflakeStreamingIngestClient clientC = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        Mockito.when(clientA.isClosed()).thenReturn(false);
//        Mockito.when(clientB.isClosed()).thenReturn(false);
//        Mockito.when(clientC.isClosed()).thenReturn(false);
//
//        // test get uninitialized clients
//        ClientManager clientManager = new ClientManager(5, 2);
//        Set<SnowflakeStreamingIngestClient> goalClients = new HashSet<>();
//        this.assertGoalClients(clientManager, goalClients);
//
//        // test add clientA
//        clientManager.upsertClient(clientATaskList, clientA);
//
//        Mockito.verify(clientA, Mockito.times(1)).isClosed();
//        goalClients.add(clientA);
//        this.assertGoalClients(clientManager, goalClients);
//
//        // test add clientB
//        clientManager.upsertClient(clientBTaskList, clientB);
//
//        Mockito.verify(clientB, Mockito.times(1)).isClosed();
//        goalClients.add(clientB);
//        this.assertGoalClients(clientManager, goalClients);
//
//        // test add clientC
//        clientManager.upsertClient(clientCTaskList, clientC);
//
//        Mockito.verify(clientC, Mockito.times(1)).isClosed();
//        goalClients.add(clientC);
//        this.assertGoalClients(clientManager, goalClients);
//
//        assert goalClients.size() == 3;
//
//        Mockito.verify(clientA, Mockito.times(4)).isClosed();
//        Mockito.verify(clientB, Mockito.times(3)).isClosed();
//        Mockito.verify(clientC, Mockito.times(2)).isClosed();
//    }
//
//    @Test
//    public void testGetAndRemoveClients() {
//        // 5 tasks, 2 tasks per client = [0, 1] -> clientA, [2, 3] -> clientB, [4] -> clientC
//        // setup
//        List<Integer> clientATaskList = Arrays.asList(0, 1);
//        List<Integer> clientBTaskList = Arrays.asList(2, 3);
//        List<Integer> clientCTaskList = Arrays.asList(4);
//
//        // since remove takes a client as a param, create a mock param client as well for mock verification accuracy
//        String clientAName = "clientA";
//        SnowflakeStreamingIngestClient paramClientA = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        SnowflakeStreamingIngestClient existingClientA = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        Mockito.when(paramClientA.getName()).thenReturn(clientAName);
//        Mockito.when(existingClientA.getName()).thenReturn(clientAName);
//        String clientBName = "clientB";
//        SnowflakeStreamingIngestClient paramClientB = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        SnowflakeStreamingIngestClient existingClientB = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        Mockito.when(paramClientB.getName()).thenReturn(clientBName);
//        Mockito.when(existingClientB.getName()).thenReturn(clientBName);
//        String clientCName = "clientC";
//        SnowflakeStreamingIngestClient paramClientC = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        SnowflakeStreamingIngestClient existingClientC = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        Mockito.when(paramClientC.getName()).thenReturn(clientCName);
//        Mockito.when(existingClientC.getName()).thenReturn(clientCName);
//
//        Set<SnowflakeStreamingIngestClient> goalClients = new HashSet<>();
//        goalClients.add(existingClientA);
//        goalClients.add(existingClientB);
//        goalClients.add(existingClientC);
//
//        ClientManager clientManager = new ClientManager(5, 2);
//        clientManager.upsertClient(clientATaskList, existingClientA);
//        clientManager.upsertClient(clientBTaskList, existingClientB);
//        clientManager.upsertClient(clientCTaskList, existingClientC);
//        this.assertGoalClients(clientManager, goalClients);
//
//        // test remove clientA
//        clientManager.removeClientTaskEntry(paramClientA);
//
//        // uses client name to double check client is correct
//        Mockito.verify(paramClientA, Mockito.times(1)).getName();
//        Mockito.verify(existingClientA, Mockito.times(1)).getName();
//        Mockito.verify(existingClientB, Mockito.times(1)).getName();
//        Mockito.verify(existingClientC, Mockito.times(1)).getName();
//        goalClients.remove(existingClientA);
//        this.assertGoalClients(clientManager, goalClients);
//
//        // test remove not initialized client should not affect anything or fail
//        SnowflakeStreamingIngestClient notInitClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
//        Mockito.when(notInitClient.getName()).thenReturn("notInitializedClient");
//        clientManager.removeClientTaskEntry(notInitClient);
//
//        Mockito.verify(notInitClient, Mockito.times(1)).getName();
//        this.assertGoalClients(clientManager, goalClients);
//
//        // test remove clientB
//        clientManager.removeClientTaskEntry(existingClientB);
//        goalClients.remove(existingClientB);
//        this.assertGoalClients(clientManager, goalClients);
//
//        // test remove clientC
//        clientManager.removeClientTaskEntry(existingClientC);
//        goalClients.remove(existingClientC);
//        this.assertGoalClients(clientManager, goalClients);
//
//        assert goalClients.size() == 0;
//
////        Mockito.verify(clientA, Mockito.times(2)).getName();
////        Mockito.verify(notInitClient, Mockito.times(2)).getName();
////        Mockito.verify(clientB, Mockito.times(3)).getName();
////        Mockito.verify(clientC, Mockito.times(3)).getName();
//    }
//
//    private void assertGoalClients(ClientManager clientManager, Set<SnowflakeStreamingIngestClient> goalClients) {
//        Set<SnowflakeStreamingIngestClient> gotClients = clientManager.getValidClients();
//
//        // verify clients are as expected
//        assert gotClients.size() == goalClients.size();
//        for (SnowflakeStreamingIngestClient goalClient : goalClients) {
//            assert gotClients.contains(goalClient);
//        }
//    }
//
//    @Test
//    public void testGetClient() {
//
//    }
//
//    @Test
//    public void testAddClient() {
//
//    }
//
//    @Test
//    public void testRemoveClient() {
//
//    }
//
//    @Test
//    public void testValidate() {
//
//    }
//}
