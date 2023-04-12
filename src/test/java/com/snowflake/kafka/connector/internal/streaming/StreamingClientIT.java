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
public class StreamingClientIT {
  private Map<String, String> clientConfig1;
  private Map<String, String> clientConfig2;
  private Map<String, String> clientConfig3;

  private StreamingClientProvider streamingClientProvider;
  private StreamingClientHandler streamingClientHandler;
  private boolean enableClientOptimization;

  @Parameterized.Parameters(name = "enableClientOptimization: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  public StreamingClientIT(boolean enableClientOptimization) {
    this.enableClientOptimization = enableClientOptimization;
  }

  @Before
  public void setup() {
    this.clientConfig1 = TestUtils.getConfForStreaming();
    this.clientConfig1.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        this.enableClientOptimization + "");
    this.clientConfig1.put(Utils.TASK_ID, "1");
    this.clientConfig1.put(Utils.NAME, "client1");
    this.clientConfig2 = new HashMap<>(this.clientConfig1);
    this.clientConfig2.put(Utils.TASK_ID, "2");
    this.clientConfig2.put(Utils.NAME, "client2");
    this.clientConfig3 = new HashMap<>(this.clientConfig1);
    this.clientConfig3.put(Utils.TASK_ID, "3");
    this.clientConfig3.put(Utils.NAME, "client3");

    this.streamingClientHandler = Mockito.spy(StreamingClientHandler.class);
    this.streamingClientProvider =
        StreamingClientProvider.injectStreamingClientProviderForTests(
            new ConcurrentHashMap<>(), null, this.streamingClientHandler);
  }

  @After
  public void tearDown() {
    this.streamingClientProvider.closeAllClients();
  }

  @Test
  public void testGetClientConcurrency() throws Exception {
    // setup three get runners
    ProviderCaller getClient1 =
        new ProviderCaller(
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig1,
            "getClient1");
    ProviderCaller getClient2 =
        new ProviderCaller(
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig2,
            "getClient2");
    ProviderCaller getClient3 =
        new ProviderCaller(
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig3,
            "getClient3");

    // try a bunch of random get calls at the same time
    getClient1.executeMethod();
    getClient1.executeMethod();
    getClient2.executeMethod();
    getClient3.executeMethod();
    getClient3.executeMethod();
    getClient2.executeMethod();
    getClient2.executeMethod();
    getClient1.executeMethod();
    getClient3.executeMethod();
    getClient3.executeMethod();
    getClient2.executeMethod();
    getClient3.executeMethod();
    getClient1.executeMethod();
    getClient2.executeMethod();

    // verify clients are valid
    SnowflakeStreamingIngestClient client1 = getClient1.getClient();
    SnowflakeStreamingIngestClient client2 = getClient2.getClient();
    SnowflakeStreamingIngestClient client3 = getClient3.getClient();

    assert StreamingClientHandler.isClientValid(client1);
    assert StreamingClientHandler.isClientValid(client2);
    assert StreamingClientHandler.isClientValid(client3);

    // verify same client if optimization is enabled
    if (this.enableClientOptimization) {
      assert client1.getName().contains("_0");
      assert client1.getName().equals(client2.getName());
      assert client2.getName().equals(client3.getName());

      Mockito.verify(this.streamingClientHandler, Mockito.times(1)).createClient(Mockito.anyMap());
    } else {
      assert !client1.getName().equals(client2.getName());
      assert !client1.getName().equals(client3.getName());
      assert !client2.getName().equals(client3.getName());

      Mockito.verify(this.streamingClientHandler, Mockito.times(3)).createClient(Mockito.anyMap());
    }
  }

  @Test
  public void testCloseClientConcurrency() throws Exception {
    ConcurrentMap<String, SnowflakeStreamingIngestClient> clientMap = new ConcurrentHashMap<>();
    SnowflakeStreamingIngestClient paramEnabledClient = null;

    // setup provider with active valid clients
    if (this.enableClientOptimization) {
      paramEnabledClient = this.streamingClientHandler.createClient(this.clientConfig1);
    } else {
      SnowflakeStreamingIngestClient client1 = this.streamingClientHandler.createClient(this.clientConfig1);
      SnowflakeStreamingIngestClient client2 = this.streamingClientHandler.createClient(this.clientConfig3);
      SnowflakeStreamingIngestClient client3 = this.streamingClientHandler.createClient(this.clientConfig2);

      clientMap.put(this.clientConfig1.get(Utils.TASK_ID), client1);
      clientMap.put(this.clientConfig2.get(Utils.TASK_ID), client2);
      clientMap.put(this.clientConfig3.get(Utils.TASK_ID), client3);
    }

    this.streamingClientProvider = StreamingClientProvider.injectStreamingClientProviderForTests(clientMap, paramEnabledClient, this.streamingClientHandler);

    // setup three close runners
    ProviderCaller getClient1 =
            new ProviderCaller(
                    ProviderMethods.CLOSE_ALL_CLIENTS,
                    this.streamingClientProvider,
                    this.clientConfig1,
                    "closeClients1");
    ProviderCaller getClient2 =
            new ProviderCaller(
                    ProviderMethods.CLOSE_ALL_CLIENTS,
                    this.streamingClientProvider,
                    this.clientConfig2,
                    "closeClients2");
    ProviderCaller getClient3 =
            new ProviderCaller(
                    ProviderMethods.CLOSE_ALL_CLIENTS,
                    this.streamingClientProvider,
                    this.clientConfig3,
                    "closeClients3");

    // try a bunch of random close calls at the same time
    getClient1.executeMethod();
    getClient1.executeMethod();
    getClient2.executeMethod();
    getClient3.executeMethod();
    getClient3.executeMethod();
    getClient2.executeMethod();
    getClient2.executeMethod();
    getClient1.executeMethod();
    getClient3.executeMethod();
    getClient3.executeMethod();
    getClient2.executeMethod();
    getClient3.executeMethod();
    getClient1.executeMethod();
    getClient2.executeMethod();

    // join threads
    getClient1.closeAllClients();
    getClient2.closeAllClients();
    getClient3.closeAllClients();

    // verify clients are closed
    if (this.enableClientOptimization) {
      assert !StreamingClientHandler.isClientValid(paramEnabledClient);
      Mockito.verify(this.streamingClientHandler, Mockito.times(1)).closeClient(paramEnabledClient);
    } else {
      clientMap.values().forEach(client -> {
        assert !StreamingClientHandler.isClientValid(client);
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

  // represents which method this caller will call
  private enum ProviderMethods {
    GET_CLIENT,
    CLOSE_ALL_CLIENTS
  }

  private class ProviderCaller implements Runnable {
    private final ProviderMethods providerMethod;

    private StreamingClientProvider streamingClientProvider;
    private Map<String, String> config;
    private SnowflakeStreamingIngestClient gotClient;
    private String name;
    private Thread thread;

    public ProviderCaller(
        ProviderMethods providerMethod,
        StreamingClientProvider provider,
        Map<String, String> config,
        String name) {
      this.providerMethod = providerMethod;

      this.streamingClientProvider = provider;
      this.config = config;
      this.name = name;
    }

    @Override
    public void run() {
      if (this.providerMethod.equals(ProviderMethods.GET_CLIENT)) {
        this.gotClient = this.streamingClientProvider.getClient(this.config);
      } else if (this.providerMethod.equals(ProviderMethods.CLOSE_ALL_CLIENTS)) {
        this.streamingClientProvider.closeAllClients();
      }
    }

    public void executeMethod() {
      this.thread = new Thread(this, this.name);
      this.thread.start();
    }

    public SnowflakeStreamingIngestClient getClient() throws Exception {
      if (!this.providerMethod.equals(ProviderMethods.GET_CLIENT)) {
        throw new Exception("ProviderCaller not configured for getClient()");
      }
      this.joinMethod();
      return this.gotClient;
    }

    public void closeAllClients() throws Exception {
      if (!this.providerMethod.equals(ProviderMethods.CLOSE_ALL_CLIENTS)) {
        throw new Exception("ProviderCaller not configured for closeAllClients()");
      }
      this.joinMethod();
    }

    private void joinMethod() {
      try {
        this.thread.join();
      } catch (InterruptedException e) {
        assert false : "Unable to join thread: " + e.getMessage();
      }
    }
  }
}
