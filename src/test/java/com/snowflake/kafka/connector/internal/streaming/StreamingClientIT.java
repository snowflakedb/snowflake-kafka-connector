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
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class StreamingClientIT {
  // note: these tests will leak clients, however the clients should autoclose, so it should be ok

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
            null, this.streamingClientHandler);
  }

  @After
  public void tearDown() {}

  @Test
  public void testGetClientConcurrency() throws Exception {
    // setup three get runners
    ProviderCaller getClient1 =
        new ProviderCaller(
            "getClient1",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig1);
    ProviderCaller getClient2 =
        new ProviderCaller(
            "getClient2",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig2);
    ProviderCaller getClient3 =
        new ProviderCaller(
            "getClient3",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig3);

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
    getClient1.executeMethod();

    // verify clients are valid
    SnowflakeStreamingIngestClient client1 = getClient1.getClient();
    SnowflakeStreamingIngestClient client2 = getClient2.getClient();
    SnowflakeStreamingIngestClient client3 = getClient3.getClient();

    assert StreamingClientHandler.isClientValid(client1);
    assert StreamingClientHandler.isClientValid(client2);
    assert StreamingClientHandler.isClientValid(client3);

    // verify same client if optimization is enabled, or all new clients
    if (this.enableClientOptimization) {
      assert client1.getName().contains("_0");
      assert client1.getName().equals(client2.getName());
      assert client2.getName().equals(client3.getName());

      Mockito.verify(this.streamingClientHandler, Mockito.times(1)).createClient(Mockito.anyMap());
    } else {
      assert !client1.getName().equals(client2.getName());
      assert !client1.getName().equals(client3.getName());
      assert !client2.getName().equals(client3.getName());

      Mockito.verify(this.streamingClientHandler, Mockito.times(4)).createClient(clientConfig1);
      Mockito.verify(this.streamingClientHandler, Mockito.times(4)).createClient(clientConfig2);
      Mockito.verify(this.streamingClientHandler, Mockito.times(4)).createClient(clientConfig3);
    }
  }

  @Test
  public void testCloseClientConcurrency() throws Exception {
    // setup provider with active valid client
    SnowflakeStreamingIngestClient closeClient =
        this.streamingClientHandler.createClient(this.clientConfig1);

    this.streamingClientProvider =
        StreamingClientProvider.injectStreamingClientProviderForTests(
            closeClient, this.streamingClientHandler);

    // setup three close runners
    ProviderCaller getClient1 =
        new ProviderCaller(
            "closeClients1",
            ProviderMethods.CLOSE_CLIENT,
            this.streamingClientProvider,
            closeClient);
    ProviderCaller getClient2 =
        new ProviderCaller(
            "closeClients2",
            ProviderMethods.CLOSE_CLIENT,
            this.streamingClientProvider,
            closeClient);
    ProviderCaller getClient3 =
        new ProviderCaller(
            "closeClients3",
            ProviderMethods.CLOSE_CLIENT,
            this.streamingClientProvider,
            closeClient);

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

    // verify client is closed
    assert !StreamingClientHandler.isClientValid(closeClient);
    Mockito.verify(this.streamingClientHandler, Mockito.times(14)).closeClient(closeClient);
  }

  // represents which method this caller will call
  private enum ProviderMethods {
    GET_CLIENT,
    CLOSE_CLIENT
  }

  private class ProviderCaller implements Runnable {
    private Thread thread;
    private String threadName;
    private final ProviderMethods providerMethod;
    private StreamingClientProvider streamingClientProvider;

    private Map<String, String> config;

    private SnowflakeStreamingIngestClient returnClient;
    private SnowflakeStreamingIngestClient closeClient;

    public ProviderCaller(
        String threadName,
        ProviderMethods providerMethod,
        StreamingClientProvider provider,
        Map<String, String> config) {
      this.providerMethod = providerMethod;
      this.threadName = threadName;
      this.streamingClientProvider = provider;

      this.config = config;
    }

    public ProviderCaller(
        String threadName,
        ProviderMethods providerMethod,
        StreamingClientProvider provider,
        SnowflakeStreamingIngestClient closeClient) {
      this.providerMethod = providerMethod;
      this.threadName = threadName;
      this.streamingClientProvider = provider;

      this.closeClient = closeClient;
    }

    @Override
    public void run() {
      if (this.providerMethod.equals(ProviderMethods.GET_CLIENT)) {
        this.returnClient = this.streamingClientProvider.getClient(this.config);
      } else if (this.providerMethod.equals(ProviderMethods.CLOSE_CLIENT)) {
        this.streamingClientProvider.closeClient(this.closeClient);
      }
    }

    public void executeMethod() {
      this.thread = new Thread(this, this.threadName);
      this.thread.start();
    }

    public SnowflakeStreamingIngestClient getClient() throws Exception {
      if (!this.providerMethod.equals(ProviderMethods.GET_CLIENT)) {
        throw new Exception("ProviderCaller not configured for getClient()");
      }
      this.joinMethod();
      return this.returnClient;
    }

    public void closeAllClients() throws Exception {
      if (!this.providerMethod.equals(ProviderMethods.CLOSE_CLIENT)) {
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
