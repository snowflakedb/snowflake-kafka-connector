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

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ClientManagerTest {
  private Map<String, String> config;
  private String kcInstanceId;
  private String clientName;
  private ClientTaskMap clientTaskMap;
  private SnowflakeStreamingIngestClient goalClient;
  private SnowflakeStreamingIngestClient mockClient;

  @Before
  // set up sunny day tests
  public void setup() {
    this.kcInstanceId = "testKcId";
    this.clientName = ClientManager.getStreamingIngestClientName(this.kcInstanceId, 0);

    // config
    this.config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(this.config);

    // client
    Map<String, String> streamingPropertiesMap =
            StreamingUtils.convertConfigForStreamingClient(new HashMap<>(this.config));
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(streamingPropertiesMap);

    this.goalClient = SnowflakeStreamingIngestClientFactory.builder(this.clientName)
            .setProperties(streamingClientProps)
            .build();
    this.mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);

    // client task map
    this.clientTaskMap = Mockito.mock(ClientTaskMap.class);
  }

  @After
  public void teardown() throws Exception {
      this.goalClient.close();
  }

  @Test
  public void testCreateAndGetClient() throws Exception {
    // setup
    List<Integer> taskList = Arrays.asList(0);
    Set<List<Integer>> taskListSet = new HashSet<>();
    taskListSet.add(taskList);

    Mockito.when(this.clientTaskMap.getTaskIdLists()).thenReturn(taskListSet);
    Mockito.when(this.clientTaskMap.getValidClient(taskList.get(0))).thenReturn(this.goalClient);
    Mockito.when(this.clientTaskMap.validateMap(1)).thenReturn("");

    // test creation
    ClientManager clientManager = new ClientManager();
    clientManager.createAllStreamingClients(this.config, this.kcInstanceId, this.clientTaskMap);

    // test get
    SnowflakeStreamingIngestClient createdClient = clientManager.getStreamingIngestClient(taskList.get(0));
    createdClient.close(); // dont orphan client

    // verify
    assert createdClient.getName().equals(goalClient.getName());

    Mockito.verify(this.clientTaskMap, Mockito.times(1)).getTaskIdLists();
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).upsertClient(Mockito.eq(taskList), ArgumentMatchers.any(SnowflakeStreamingIngestClient.class));
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).validateMap(1);
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).getValidClient(taskList.get(0));
  }

  @Test
  public void testCreateClientInvalidMap() {
    // setup
    List<Integer> taskList = Arrays.asList(0);
    Set<List<Integer>> taskListSet = new HashSet<>();
    taskListSet.add(taskList);
    String exceptionMsg = "Invalid map!";
    Mockito.when(this.clientTaskMap.getTaskIdLists()).thenReturn(taskListSet);

    // test creation
    Mockito.when(this.clientTaskMap.validateMap(1)).thenReturn(exceptionMsg);

    ClientManager clientManager = new ClientManager();
    assert TestUtils.assertError(
            SnowflakeErrors.ERROR_3009,
            () -> {
              clientManager.createAllStreamingClients(this.config, this.kcInstanceId, this.clientTaskMap);
            });

    // verify
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).getTaskIdLists();
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).upsertClient(Mockito.eq(taskList), ArgumentMatchers.any(SnowflakeStreamingIngestClient.class));
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).validateMap(1);
  }

  @Test
  public void testCreateClientFailure() {
    // setup
    List<Integer> taskList = Arrays.asList(0);
    Set<List<Integer>> taskListSet = new HashSet<>();
    taskListSet.add(taskList);
    Mockito.when(this.clientTaskMap.getTaskIdLists()).thenReturn(taskListSet);

    // test creation
    this.config.remove(Utils.SF_ROLE);

    ClientManager clientManager = new ClientManager();
    try {
      clientManager.createAllStreamingClients(this.config, this.kcInstanceId, this.clientTaskMap);
    } catch (ConnectException ex) {
      assert ex.getCause() instanceof SFException;
      assert ex.getCause().getMessage().contains("Missing role");
    }

    // verify
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).getTaskIdLists();
  }

  @Test
  public void testCloseClient() throws Exception {
    // setup
    List<Integer> taskList = Arrays.asList(0);
    Set<List<Integer>> taskListSet = new HashSet<>();
    taskListSet.add(taskList);
    Set<SnowflakeStreamingIngestClient> clientSet = new HashSet<>();
    clientSet.add(this.mockClient);
    Mockito.when(this.clientTaskMap.getValidClients()).thenReturn(clientSet);
    Mockito.when(this.clientTaskMap.validateMap(0)).thenReturn("");

    // test
    ClientManager clientManager = new ClientManager(this.clientTaskMap, 1);
    clientManager.closeAllStreamingClients();

    // verify client closed
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).getValidClients();
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).removeClientTaskEntry(ArgumentMatchers.any(SnowflakeStreamingIngestClient.class));
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).validateMap(0);
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
    Mockito.verify(this.mockClient, Mockito.times(1)).close();
  }

  // note: we dont throw exceptions on close
  @Test
  public void testCloseClientInvalidMap() throws Exception {
    // setup
    List<Integer> taskList = Arrays.asList(0);
    Set<List<Integer>> taskListSet = new HashSet<>();
    taskListSet.add(taskList);
    Set<SnowflakeStreamingIngestClient> clientSet = new HashSet<>();
    clientSet.add(this.mockClient);
    Mockito.when(this.clientTaskMap.getValidClients()).thenReturn(clientSet);

    // test
    Mockito.when(this.clientTaskMap.validateMap(0)).thenReturn("invalid map!");

    ClientManager clientManager = new ClientManager(this.clientTaskMap, 1);
    clientManager.closeAllStreamingClients();

    // verify client closed
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).getValidClients();
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).removeClientTaskEntry(ArgumentMatchers.any(SnowflakeStreamingIngestClient.class));
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).validateMap(0);
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
    Mockito.verify(this.mockClient, Mockito.times(1)).close();
  }


  // note: we dont throw exceptions on close
  @Test
  public void testCloseClientFailures() throws Exception {
    // setup
    Set<SnowflakeStreamingIngestClient> clientSet = new HashSet<>();
    clientSet.add(this.mockClient);
    Mockito.when(this.clientTaskMap.getValidClients()).thenReturn(clientSet);
    Mockito.when(this.clientTaskMap.validateMap(0)).thenReturn("");

    // test cant close client no message
    Exception thrownException = new Exception();
    Mockito.doThrow(thrownException).when(this.mockClient).close();
    ClientManager clientManager = new ClientManager(this.clientTaskMap, 1);
    clientManager.closeAllStreamingClients();

    // test cant close client with message
    thrownException = new Exception("don't forget to pet a cat today");
    Mockito.doThrow(thrownException).when(this.mockClient).close();
    clientManager = new ClientManager(this.clientTaskMap, 1);
    clientManager.closeAllStreamingClients();

    // test cant close client no cause
    thrownException.initCause(new Exception());
    Mockito.doThrow(thrownException).when(this.mockClient).close();
    clientManager = new ClientManager(this.clientTaskMap, 1);
    clientManager.closeAllStreamingClients();

    // verify client closed
    Mockito.verify(this.clientTaskMap, Mockito.times(3)).getValidClients();
    Mockito.verify(this.clientTaskMap, Mockito.times(3)).removeClientTaskEntry(ArgumentMatchers.any(SnowflakeStreamingIngestClient.class));
    Mockito.verify(this.clientTaskMap, Mockito.times(3)).validateMap(0);
    Mockito.verify(this.mockClient, Mockito.times(3)).isClosed();
    Mockito.verify(this.mockClient, Mockito.times(3)).close();
  }

  // note: we dont throw exceptions on close
  @Test
  public void testCloseAlreadyClosedClient() throws Exception {
    // setup
    Set<SnowflakeStreamingIngestClient> clientSet = new HashSet<>();
    clientSet.add(this.mockClient);
    Mockito.when(this.clientTaskMap.getValidClients()).thenReturn(clientSet);
    Mockito.when(this.clientTaskMap.validateMap(0)).thenReturn("");

    // test client is already closed
    Mockito.when(this.mockClient.isClosed()).thenReturn(true);

    ClientManager clientManager = new ClientManager(this.clientTaskMap, 1);
    clientManager.closeAllStreamingClients();

    // verify client closed
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).getValidClients();
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).removeClientTaskEntry(ArgumentMatchers.any(SnowflakeStreamingIngestClient.class));
    Mockito.verify(this.clientTaskMap, Mockito.times(1)).validateMap(0);
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
  }

//  @Test
//  public void testCloseNullClient() {
//    // setup
//    ClientManager clientManager = new ClientManager();
//
//    // test
//    clientManager.closeAllStreamingClients();
//
//    // verify client closed
//    assert isClosed;
//  }
//
//  @Test
//  public void testAlreadyClosedClient() throws Exception {
//    // setup
//    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
//    Mockito.when(goalClient.isClosed()).thenReturn(true);
//    ClientManager clientManager = new ClientManager();
//
//    // test
//    boolean isClosed = clientManager.closeAllStreamingClients();
//
//    // verify client closed
//    assert isClosed;
//    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
//    Mockito.verify(goalClient, Mockito.times(0)).close();
//  }
//
//  @Test
//  public void testCloseClientExceptionNoMessage() throws Exception {
//    // setup
//    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
//    ClientManager clientManager = new ClientManager();
//    Mockito.doThrow(new Exception()).when(goalClient).close();
//
//    // test
//    boolean isClosed = clientManager.closeAllStreamingClients();
//
//    // verify
//    assert !isClosed;
//    Mockito.verify(goalClient, Mockito.times(1)).close();
//    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
//  }
//
//  @Test
//  public void testCloseClientExceptionNoCause() throws Exception {
//    // setup
//    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
//    ClientManager clientManager = new ClientManager();
//    Mockito.doThrow(new Exception("test close client failure exception")).when(goalClient).close();
//
//    // test
//    boolean isClosed = clientManager.closeAllStreamingClients();
//
//    // verify
//    assert !isClosed;
//    Mockito.verify(goalClient, Mockito.times(1)).close();
//    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
//  }
//
//  @Test
//  public void testCloseClientException() throws Exception {
//    // setup
//    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
//    ClientManager clientManager = new ClientManager();
//    Exception closeException = new Exception("test close client failure exception");
//    Exception causeException = new Exception("cause exception");
//    closeException.initCause(causeException);
//    Mockito.doThrow(closeException).when(goalClient).close();
//
//    // test
//    boolean isClosed = clientManager.closeAllStreamingClients();
//
//    // verify
//    assert !isClosed;
//    Mockito.verify(goalClient, Mockito.times(1)).close();
//    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
//  }
//
//  @Test
//  public void testGetClientFailure() {
//    ClientManager clientManager = new ClientManager(null);
//    assert TestUtils.assertError(
//        SnowflakeErrors.ERROR_3009,
//        () -> {
//          clientManager.getStreamingIngestClient(0);
//        });
//  }
//
//  @Test(expected = ConnectException.class)
//  public void testMissingPropertiesForStreamingClient() {
//    this.config.remove(Utils.SF_ROLE);
//    ClientManager clientManager = new ClientManager(null);
//
//    try {
//      clientManager.createStreamingClient(this.config, kcInstanceId);
//    } catch (ConnectException ex) {
//      assert ex.getCause() instanceof SFException;
//      assert ex.getCause().getMessage().contains("Missing role");
//      throw ex;
//    }
//  }
}
