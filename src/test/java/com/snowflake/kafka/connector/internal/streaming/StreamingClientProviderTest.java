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

package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProvider.injectStreamingClientProviderForTests;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class StreamingClientProviderTest {
  private Map<String, String> clientConfig1;
  private Map<String, String> clientConfig2;
  private Map<String, String> clientConfig3;

//  private SnowflakeStreamingIngestClient parameterEnabledClient;
//
//  private SnowflakeStreamingIngestClient streamingIngestClient1;
//  private SnowflakeStreamingIngestClient streamingIngestClient2;
//  private SnowflakeStreamingIngestClient streamingIngestClient3;

  private StreamingClientProvider streamingClientProvider;
  private StreamingClientHandler streamingClientHandler;
  private boolean enableClientOptimization;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  public StreamingClientProviderTest(boolean enableClientOptimization) {
    this.enableClientOptimization = enableClientOptimization;
  }

  @Before
  public void setup() {
    // setup fresh configs
    this.clientConfig1 = TestUtils.getConfForStreaming();
    this.clientConfig1.put(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, this.enableClientOptimization + "");
    this.clientConfig1.put(Utils.TASK_ID, "1");
    this.clientConfig1.put(Utils.NAME, "client1");
    this.clientConfig2 = new HashMap<>(this.clientConfig1);
    this.clientConfig2.put(Utils.TASK_ID, "2");
    this.clientConfig2.put(Utils.NAME, "client2");
    this.clientConfig3 = new HashMap<>(this.clientConfig1);
    this.clientConfig3.put(Utils.TASK_ID, "3");
    this.clientConfig3.put(Utils.NAME, "client3");

    this.streamingClientHandler = Mockito.spy(StreamingClientHandler.class);
    this.streamingClientProvider = StreamingClientProvider.injectStreamingClientProviderForTests(new ConcurrentHashMap<>(), null, this.streamingClientHandler);
  }

  @After
  public void cleanUpProviderClient() {
    // note that the config will not be cleaned up
    this.streamingClientProvider.closeAllClients();
  }

  @Test
  public void testFirstGetClient() {
    // test actual provider
    SnowflakeStreamingIngestClient createdClient =
        this.streamingClientProvider.getClient(this.clientConfig1);

    // verify - should create a client regardless of optimization
    assert StreamingClientHandler.isClientValid(createdClient);
    createdClient.getName().contains(this.clientConfig1.get(Utils.NAME));
    Mockito.verify(this.streamingClientHandler, Mockito.times(1))
            .createClient(this.clientConfig1);
  }

  @Test
  public void testGetInvalidClient() {
    String invalidClientName = "invalid client";
    String validClientName = "valid client";

    // setup invalid client
    this.clientConfig1.put(Utils.NAME, invalidClientName);
    SnowflakeStreamingIngestClient invalidClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(invalidClient.isClosed()).thenReturn(true);

    ConcurrentMap<String, SnowflakeStreamingIngestClient> clientMap = new ConcurrentHashMap<>();
    SnowflakeStreamingIngestClient injectedClient = null;

    // setup invalid client state based on optimization
    if (this.enableClientOptimization) {
      injectedClient = invalidClient;
    } else {
      clientMap.put(this.clientConfig1.get(Utils.TASK_ID), invalidClient);
    }

    StreamingClientProvider injectedProvider = injectStreamingClientProviderForTests(clientMap, injectedClient, this.streamingClientHandler);

    // test: getting invalid client
    this.clientConfig1.put(Utils.NAME, validClientName);
    SnowflakeStreamingIngestClient recreatedClient =
        injectedProvider.getClient(this.clientConfig1);

    // verify: created valid client
    assert StreamingClientHandler.isClientValid(recreatedClient);
    assert recreatedClient.getName().contains(validClientName);
    assert !recreatedClient.getName().contains(invalidClientName);
    Mockito.verify(this.streamingClientHandler, Mockito.times(1))
        .createClient(this.clientConfig1);

    // verify: invalid client was closed
    Mockito.verify(invalidClient, Mockito.times(1))
            .isClosed();
  }

  @Test
  public void testGetExistingClient() {
    // test
    SnowflakeStreamingIngestClient client1 = this.streamingClientProvider.getClient(this.clientConfig1);
    SnowflakeStreamingIngestClient client2 = this.streamingClientProvider.getClient(this.clientConfig2);
    SnowflakeStreamingIngestClient client3 = this.streamingClientProvider.getClient(this.clientConfig3);
    SnowflakeStreamingIngestClient client4 = this.streamingClientProvider.getClient(this.clientConfig1);

    // verify: clients are valid
    assert StreamingClientHandler.isClientValid(client1);
    assert StreamingClientHandler.isClientValid(client2);
    assert StreamingClientHandler.isClientValid(client3);
    assert StreamingClientHandler.isClientValid(client3);

    // verify: clients should be the same if optimization is enabled
    if (this.enableClientOptimization) {
      assert client1.getName().equals(client2.getName());
      assert client1.getName().equals(client3.getName());
      assert client1.getName().equals(client4.getName());
      assert client1.getName().contains(this.clientConfig1.get(Utils.NAME));
    } else {
      // since client4 was got with client1's taskid, it should be the same
      assert client1.getName().equals(client4.getName());

      assert !client1.getName().equals(client2.getName());
      assert !client2.getName().equals(client3.getName());
      assert !client1.getName().equals(client3.getName());
      assert client1.getName().contains(this.clientConfig1.get(Utils.NAME));
      assert client2.getName().contains(this.clientConfig2.get(Utils.NAME));
      assert client3.getName().contains(this.clientConfig3.get(Utils.NAME));
    }
  }

  @Test
  public void testCloseAllClients() throws Exception {
    ConcurrentMap<String, SnowflakeStreamingIngestClient> clientMap = new ConcurrentHashMap<>();
    SnowflakeStreamingIngestClient parameterEnabledClient = null;

    // setup: if optimized, there should only be one client
    if (this.enableClientOptimization) {
      parameterEnabledClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    } else {
      SnowflakeStreamingIngestClient client1 = Mockito.mock(SnowflakeStreamingIngestClient.class);
      SnowflakeStreamingIngestClient client2 = Mockito.mock(SnowflakeStreamingIngestClient.class);
      SnowflakeStreamingIngestClient client3 = Mockito.mock(SnowflakeStreamingIngestClient.class);
      clientMap.put(this.clientConfig1.get(Utils.TASK_ID), client1);
      clientMap.put(this.clientConfig2.get(Utils.TASK_ID), client2);
      clientMap.put(this.clientConfig3.get(Utils.TASK_ID), client3);
    }

    // test closing all clients
    StreamingClientProvider injectedProvider =
            injectStreamingClientProviderForTests(clientMap, parameterEnabledClient, this.streamingClientHandler);

    injectedProvider.closeAllClients();

    // verify: if optimized, there should only be one closeClient() call
    if (this.enableClientOptimization) {
      Mockito.verify(this.streamingClientHandler, Mockito.times(1)).closeClient(parameterEnabledClient);
    } else {
      clientMap.values().forEach(client -> {
        Mockito.verify(this.streamingClientHandler, Mockito.times(1)).closeClient(client);
      });
    }
  }

//  @Test
//  public void testCloseAllClientsInvalid() {}
//
//  @Test
//  public void testCloseInvalidClient() throws Exception {
//    // inject invalid client
//    SnowflakeStreamingIngestClient streamingIngestClient =
//        Mockito.mock(SnowflakeStreamingIngestClient.class);
//    Mockito.when(streamingIngestClient.isClosed()).thenReturn(true);
//    StreamingClientProvider injectedProvider =
//        injectStreamingClientProviderForTests(
//            new ConcurrentHashMap<>(), streamingIngestClient, this.streamingClientHandler);
//
//    // try closing client
//    injectedProvider.closeAllClients();
//
//    // verify didn't call close
//    Mockito.verify(streamingIngestClient, Mockito.times(0)).close();
//  }
//
//  // PARALLELISM TESTS
//
//  @Test
//  public void testMultiThreadGetEnabledParam() {
//    String clientName = "clientName";
//    int clientId = 0;
//
//    // setup
//    this.clientConfig1.put(Utils.NAME, clientName);
//    StreamingClientProvider injectedProvider =
//        injectStreamingClientProviderForTests(
//            new ConcurrentHashMap<>(), null, this.streamingClientHandler);
//
//    GetClientRunnable getClientRunnable1 =
//        new GetClientRunnable(injectedProvider, this.clientConfig1, "getClientRunnable1");
//    GetClientRunnable getClientRunnable2 =
//        new GetClientRunnable(injectedProvider, this.clientConfig1, "getClientRunnable2");
//    GetClientRunnable getClientRunnable3 =
//        new GetClientRunnable(injectedProvider, this.clientConfig1, "getClientRunnable3");
//
//    // get client on multiple threads
//    getClientRunnable1.start();
//    getClientRunnable2.start();
//    getClientRunnable3.start();
//
//    // verify same client
//    SnowflakeStreamingIngestClient client1 = getClientRunnable1.getClient();
//    SnowflakeStreamingIngestClient client2 = getClientRunnable2.getClient();
//    SnowflakeStreamingIngestClient client3 = getClientRunnable3.getClient();
//
//    assert client1.getName().contains(clientName);
//    assert client1.getName().equals(client2.getName());
//    assert client2.getName().equals(client3.getName());
//  }
//
//  @Test
//  public void testMultiThreadGetDisabledParam() {
//    this.clientConfig1.put(
//        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "false");
//    String clientName = this.clientConfig1.get(Utils.NAME);
//
//    // setup
//    String taskId1 = "1";
//    String taskId2 = "2";
//    String taskId3 = "3";
//
//    Map<String, String> client1Config = new HashMap<>(this.clientConfig1);
//    client1Config.put(Utils.TASK_ID, taskId1);
//    Map<String, String> client2Config = new HashMap<>(this.clientConfig1);
//    client2Config.put(Utils.TASK_ID, taskId2);
//    Map<String, String> client3Config = new HashMap<>(this.clientConfig1);
//    client3Config.put(Utils.TASK_ID, taskId3);
//
//    StreamingClientProvider injectedProvider =
//        injectStreamingClientProviderForTests(
//            new ConcurrentHashMap<>(), null, this.streamingClientHandler);
//
//    GetClientRunnable getClientRunnable1 =
//        new GetClientRunnable(injectedProvider, client1Config, "getClientRunnable1");
//    GetClientRunnable getClientRunnable2 =
//        new GetClientRunnable(injectedProvider, client2Config, "getClientRunnable2");
//    GetClientRunnable getClientRunnable3 =
//        new GetClientRunnable(injectedProvider, client3Config, "getClientRunnable3");
//
//    // get client on multiple threads
//    getClientRunnable1.start();
//    getClientRunnable2.start();
//    getClientRunnable3.start();
//
//    // verify different client
//    SnowflakeStreamingIngestClient resClient1 = getClientRunnable1.getClient();
//    SnowflakeStreamingIngestClient resClient2 = getClientRunnable2.getClient();
//    SnowflakeStreamingIngestClient resClient3 = getClientRunnable3.getClient();
//
//    assert resClient1.getName().contains(clientName);
//    assert resClient2.getName().contains(clientName);
//    assert resClient3.getName().contains(clientName);
//    assert !resClient1.getName().equals(resClient2.getName());
//    assert !resClient2.getName().equals(resClient3.getName());
//    assert !resClient3.getName().equals(resClient1.getName());
//  }
//
//  private class GetClientRunnable implements Runnable {
//    private StreamingClientProvider streamingClientProvider;
//    private Map<String, String> config;
//    private SnowflakeStreamingIngestClient gotClient;
//    private String name;
//    private Thread thread;
//
//    public GetClientRunnable(
//        StreamingClientProvider provider, Map<String, String> config, String name) {
//      this.streamingClientProvider = provider;
//      this.config = config;
//      this.name = name;
//    }
//
//    @Override
//    public void run() {
//      this.gotClient = this.streamingClientProvider.getClient(this.config);
//    }
//
//    public SnowflakeStreamingIngestClient getClient() {
//      try {
//        this.thread.join();
//      } catch (InterruptedException e) {
//        assert false : "Unable to join thread: " + e.getMessage();
//      }
//
//      return this.gotClient;
//    }
//
//    public void start() {
//      this.thread = new Thread(this, this.name);
//      this.thread.start();
//    }
//  }
}
