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

  @Test
  public void testMultipleGetAndClose() throws Exception {
    // setup these counts outside of the runner to not mess with that paralleism
    int getCount1 = 0;
    int getCount2 = 0;
    int getCount3 = 0;
    int closeCount1 = 0;
    int closeCount2 = 0;
    int closeCount3 = 0;

    ProviderCaller getClientCaller1 =
        new ProviderCaller(
            "getClient1",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig1);
    ProviderCaller getClientCaller2 =
        new ProviderCaller(
            "getClient2",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig2);
    ProviderCaller getClientCaller3 =
        new ProviderCaller(
            "getClient3",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig3);

    // a bunch of random get calls
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);

    SnowflakeStreamingIngestClient client1 = getClientCaller1.getClient();
    SnowflakeStreamingIngestClient client2 = getClientCaller2.getClient();
    SnowflakeStreamingIngestClient client3 = getClientCaller3.getClient();

    // create close callers from got clients
    ProviderCaller closeClientCaller1 =
        new ProviderCaller(
            "closeClient1", ProviderMethods.CLOSE_CLIENT, this.streamingClientProvider, client1);
    ProviderCaller closeClientCaller2 =
        new ProviderCaller(
            "closeClient2", ProviderMethods.CLOSE_CLIENT, this.streamingClientProvider, client2);
    ProviderCaller closeClientCaller3 =
        new ProviderCaller(
            "closeClient3", ProviderMethods.CLOSE_CLIENT, this.streamingClientProvider, client3);

    // test: get calls interleaved with close calls
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);
    closeClientCaller3 =
        this.executeCloseAfterGet(closeClientCaller3, getClientCaller3.getClient());
    closeCount3++;
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    closeClientCaller1 =
        this.executeCloseAfterGet(closeClientCaller1, getClientCaller1.getClient());
    closeCount1++;
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    closeClientCaller3 =
        this.executeCloseAfterGet(closeClientCaller3, getClientCaller3.getClient());
    closeCount3++;
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);
    closeClientCaller3 =
        this.executeCloseAfterGet(closeClientCaller3, getClientCaller3.getClient());
    closeCount3++;
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    closeClientCaller2 =
        this.executeCloseAfterGet(closeClientCaller2, getClientCaller2.getClient());
    closeCount2++;
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);

    // join all threads
    getClientCaller1.getClient();
    getClientCaller2.getClient();
    getClientCaller3.getClient();
    closeClientCaller1.closeAllClients();
    closeClientCaller2.closeAllClients();
    closeClientCaller3.closeAllClients();

    // verification
    int totalGetCount = getCount1 + getCount2 + getCount3;
    int totalCloseCount = closeCount1 + closeCount2 + closeCount3;

    assert totalGetCount > totalCloseCount;

    if (this.enableClientOptimization) {
      // close count should equal create count, even though get client was called
      Mockito.verify(this.streamingClientHandler, Mockito.times(totalCloseCount))
          .createClient(Mockito.anyMap());
      Mockito.verify(this.streamingClientHandler, Mockito.times(totalCloseCount))
          .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));
    } else {
      // should create as many as we tried to get
      Mockito.verify(this.streamingClientHandler, Mockito.times(getCount1))
          .createClient(clientConfig1);
      Mockito.verify(this.streamingClientHandler, Mockito.times(getCount2))
          .createClient(clientConfig2);
      Mockito.verify(this.streamingClientHandler, Mockito.times(getCount3))
          .createClient(clientConfig3);

      Mockito.verify(this.streamingClientHandler, Mockito.times(totalGetCount))
          .createClient(Mockito.anyMap());
      Mockito.verify(this.streamingClientHandler, Mockito.times(totalCloseCount))
          .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));
    }
  }

  @Test
  public void testGetClientConcurrency() throws Exception {
    // setup these counts outside of the runner to not mess with that paralleism
    int getCount1 = 0;
    int getCount2 = 0;
    int getCount3 = 0;

    // setup three get runners
    ProviderCaller getClientCaller1 =
        new ProviderCaller(
            "getClient1",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig1);
    ProviderCaller getClientCaller2 =
        new ProviderCaller(
            "getClient2",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig2);
    ProviderCaller getClientCaller3 =
        new ProviderCaller(
            "getClient3",
            ProviderMethods.GET_CLIENT,
            this.streamingClientProvider,
            this.clientConfig3);

    // try a bunch of random get calls at the same time
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);
    getCount2 = this.executeMethodWithCount(getClientCaller2, getCount2);
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);
    getCount3 = this.executeMethodWithCount(getClientCaller3, getCount3);
    getCount1 = this.executeMethodWithCount(getClientCaller1, getCount1);

    // verify clients are valid
    SnowflakeStreamingIngestClient client1 = getClientCaller1.getClient();
    SnowflakeStreamingIngestClient client2 = getClientCaller2.getClient();
    SnowflakeStreamingIngestClient client3 = getClientCaller3.getClient();

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

      Mockito.verify(this.streamingClientHandler, Mockito.times(getCount1))
          .createClient(clientConfig1);
      Mockito.verify(this.streamingClientHandler, Mockito.times(getCount2))
          .createClient(clientConfig2);
      Mockito.verify(this.streamingClientHandler, Mockito.times(getCount3))
          .createClient(clientConfig3);
    }
  }

  @Test
  public void testCloseClientConcurrency() throws Exception {
    // setup these counts outside of the runner to not mess with that paralleism
    int closeCount1 = 0;
    int closeCount2 = 0;
    int closeCount3 = 0;

    // setup provider with active valid client
    SnowflakeStreamingIngestClient closeClient1 =
        this.streamingClientHandler.createClient(this.clientConfig1);
    SnowflakeStreamingIngestClient closeClient2 = closeClient1;
    SnowflakeStreamingIngestClient closeClient3 = closeClient1;

    if (this.enableClientOptimization) {
      this.streamingClientProvider =
          StreamingClientProvider.injectStreamingClientProviderForTests(
              closeClient1, this.streamingClientHandler);
    } else {
      closeClient2 = this.streamingClientHandler.createClient(this.clientConfig2);
      closeClient3 = this.streamingClientHandler.createClient(this.clientConfig3);
    }

    // setup three close runners
    ProviderCaller closeClientCaller1 =
        new ProviderCaller(
            "closeClient1",
            ProviderMethods.CLOSE_CLIENT,
            this.streamingClientProvider,
            closeClient1);
    ProviderCaller closeClientCaller2 =
        new ProviderCaller(
            "closeClient2",
            ProviderMethods.CLOSE_CLIENT,
            this.streamingClientProvider,
            closeClient2);
    ProviderCaller closeClientCaller3 =
        new ProviderCaller(
            "closeClient3",
            ProviderMethods.CLOSE_CLIENT,
            this.streamingClientProvider,
            closeClient3);

    // try a bunch of random close calls at the same time
    closeCount1 = this.executeMethodWithCount(closeClientCaller1, closeCount1);
    closeCount2 = this.executeMethodWithCount(closeClientCaller2, closeCount2);
    closeCount3 = this.executeMethodWithCount(closeClientCaller3, closeCount3);
    closeCount2 = this.executeMethodWithCount(closeClientCaller2, closeCount2);
    closeCount2 = this.executeMethodWithCount(closeClientCaller2, closeCount2);
    closeCount1 = this.executeMethodWithCount(closeClientCaller1, closeCount1);
    closeCount3 = this.executeMethodWithCount(closeClientCaller3, closeCount3);
    closeCount3 = this.executeMethodWithCount(closeClientCaller3, closeCount3);
    closeCount2 = this.executeMethodWithCount(closeClientCaller2, closeCount2);
    closeCount1 = this.executeMethodWithCount(closeClientCaller1, closeCount1);

    // join threads
    closeClientCaller1.closeAllClients();
    closeClientCaller2.closeAllClients();
    closeClientCaller3.closeAllClients();

    // verify client is closed
    assert !StreamingClientHandler.isClientValid(closeClient1);
    assert !StreamingClientHandler.isClientValid(closeClient2);
    assert !StreamingClientHandler.isClientValid(closeClient3);

    if (this.enableClientOptimization) {
      Mockito.verify(
              this.streamingClientHandler, Mockito.times(closeCount1 + closeCount2 + closeCount3))
          .closeClient(closeClient1);
    } else {
      Mockito.verify(this.streamingClientHandler, Mockito.times(closeCount1))
          .closeClient(closeClient1);
      Mockito.verify(this.streamingClientHandler, Mockito.times(closeCount2))
          .closeClient(closeClient2);
      Mockito.verify(this.streamingClientHandler, Mockito.times(closeCount3))
          .closeClient(closeClient3);
    }
  }

  private int executeMethodWithCount(ProviderCaller caller, int inCount) {
    caller.executeMethod();
    return inCount + 1;
  }

  private ProviderCaller executeCloseAfterGet(
      ProviderCaller closeCaller, SnowflakeStreamingIngestClient client) {
    ProviderCaller newCaller =
        new ProviderCaller(
            closeCaller.threadName,
            closeCaller.providerMethod,
            closeCaller.streamingClientProvider,
            client);

    newCaller.executeMethod();
    return newCaller;
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
