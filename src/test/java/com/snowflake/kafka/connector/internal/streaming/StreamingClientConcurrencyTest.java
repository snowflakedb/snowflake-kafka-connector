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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class StreamingClientConcurrencyTest {
  private Map<String, String> clientConfig1;
  private Map<String, String> clientConfig2;
  private Map<String, String> clientConfig3;

  private StreamingClientProvider streamingClientProvider;
  private StreamingClientHandler streamingClientHandler;
  private boolean enableClientOptimization;

  private List<ProviderCaller> getCallers;
  private List<ProviderCaller> closeCallers;

  @Parameterized.Parameters(name = "enableClientOptimization: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  public StreamingClientConcurrencyTest(boolean enableClientOptimization) {
    this.enableClientOptimization = enableClientOptimization;
  }

  @Before
  public void setup() {
    this.clientConfig1 = TestUtils.getConfForStreaming();
    this.clientConfig1.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        this.enableClientOptimization + "");

    this.clientConfig2 = new HashMap<>(this.clientConfig1);
    this.clientConfig3 = new HashMap<>(this.clientConfig1);

    this.clientConfig1.put(Utils.NAME, "client1");
    this.clientConfig2.put(Utils.NAME, "client2");
    this.clientConfig3.put(Utils.NAME, "client3");

    this.streamingClientHandler = Mockito.spy(StreamingClientHandler.class);
    this.streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            null, this.streamingClientHandler);

    this.getCallers = new ArrayList<>();
    this.closeCallers = new ArrayList<>();
  }

  @After
  public void tearDown() {
    this.getClientStop(this.getCallers);
    this.closeClientStop(this.closeCallers);
    this.getCallers.forEach(caller -> this.streamingClientHandler.closeClient(caller.getClient()));
  }

  @Test
  public void testMultipleGetAndClose() throws Exception {
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
    this.getClientStart(getClientCaller1);
    this.getClientStart(getClientCaller2);
    this.getClientStart(getClientCaller2);
    this.getClientStart(getClientCaller3);
    this.getClientStart(getClientCaller1);
    this.getClientStart(getClientCaller2);
    this.getClientStart(getClientCaller3);

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
    getClientCaller1 = this.getClientStart(getClientCaller1);
    closeClientCaller1 = this.closeClientStart(closeClientCaller1, getClientCaller1.getClient());
    getClientCaller2 = this.getClientStart(getClientCaller2);
    getClientCaller2 = this.getClientStart(getClientCaller2);
    getClientCaller3 = this.getClientStart(getClientCaller3);
    getClientCaller3 = this.getClientStart(getClientCaller3);
    closeClientCaller3 = this.closeClientStart(closeClientCaller3, getClientCaller3.getClient());
    getClientCaller3 = this.getClientStart(getClientCaller3);
    closeClientCaller1 = this.closeClientStart(closeClientCaller1, getClientCaller1.getClient());
    getClientCaller3 = this.getClientStart(getClientCaller3);
    getClientCaller1 = this.getClientStart(getClientCaller1);
    getClientCaller2 = this.getClientStart(getClientCaller2);
    closeClientCaller2 = this.closeClientStart(closeClientCaller2, getClientCaller2.getClient());
    getClientCaller3 = this.getClientStart(getClientCaller3);

    // join all threads
    this.getClientStop(this.getCallers);
    this.closeClientStop(this.closeCallers);

    // verification
    int totalGetCount = this.getCallers.size();
    int totalCloseCount = this.closeCallers.size();

    assert totalGetCount > totalCloseCount;

    // should create as many times as it closes if param is enabled
    this.getClientStop(this.getCallers)
        .forEach(client -> StreamingClientHandler.isClientValid(client));
    Mockito.verify(
            this.streamingClientHandler,
            Mockito.times(this.enableClientOptimization ? totalCloseCount : totalGetCount))
        .createClient(Mockito.anyMap());
    Mockito.verify(this.streamingClientHandler, Mockito.times(totalCloseCount))
        .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));
  }

  @Test
  public void testGetClientConcurrency() throws Exception {
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
    this.getClientStart(getClientCaller1);
    this.getClientStart(getClientCaller2);
    this.getClientStart(getClientCaller2);
    this.getClientStart(getClientCaller3);
    this.getClientStart(getClientCaller1);
    this.getClientStart(getClientCaller2);
    this.getClientStart(getClientCaller3);
    this.getClientStart(getClientCaller1);
    this.getClientStart(getClientCaller2);
    this.getClientStart(getClientCaller3);

    // should create just once if param is enabled
    this.getClientStop(this.getCallers)
        .forEach(client -> Assert.assertTrue(StreamingClientHandler.isClientValid(client)));
    Mockito.verify(
            this.streamingClientHandler,
            Mockito.times(this.enableClientOptimization ? 1 : this.getCallers.size()))
        .createClient(Mockito.anyMap());
  }

  @Test
  public void testCloseClientConcurrency() throws Exception {
    // setup provider with active valid client
    SnowflakeStreamingIngestClient closeClient1 =
        this.streamingClientHandler.createClient(this.clientConfig1);
    SnowflakeStreamingIngestClient closeClient2 = closeClient1;
    SnowflakeStreamingIngestClient closeClient3 = closeClient1;

    if (this.enableClientOptimization) {
      this.streamingClientProvider =
          StreamingClientProvider.getStreamingClientProviderForTests(
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
    this.closeClientStart(closeClientCaller1, closeClient1);
    this.closeClientStart(closeClientCaller2, closeClient2);
    this.closeClientStart(closeClientCaller1, closeClient1);
    this.closeClientStart(closeClientCaller3, closeClient3);
    this.closeClientStart(closeClientCaller1, closeClient1);
    this.closeClientStart(closeClientCaller1, closeClient1);
    this.closeClientStart(closeClientCaller2, closeClient2);
    this.closeClientStart(closeClientCaller2, closeClient2);
    this.closeClientStart(closeClientCaller2, closeClient2);
    this.closeClientStart(closeClientCaller3, closeClient3);

    // join threads
    this.closeClientStop(this.closeCallers);

    // verify client is closed
    assert !StreamingClientHandler.isClientValid(closeClient1);
    assert !StreamingClientHandler.isClientValid(closeClient2);
    assert !StreamingClientHandler.isClientValid(closeClient3);

    if (this.enableClientOptimization) {
      Mockito.verify(this.streamingClientHandler, Mockito.times(this.closeCallers.size()))
          .closeClient(closeClient1);
    } else {
      Mockito.verify(this.streamingClientHandler, Mockito.times(this.closeCallers.size()))
          .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));
    }
  }

  private ProviderCaller getClientStart(ProviderCaller getCaller) {
    getCaller.executeMethod();
    this.getCallers.add(getCaller);

    return getCaller;
  }

  private List<SnowflakeStreamingIngestClient> getClientStop(List<ProviderCaller> callers) {
    List<SnowflakeStreamingIngestClient> resList = new ArrayList<>();
    callers.forEach(caller -> resList.add(caller.getClient()));
    return resList;
  }

  private ProviderCaller closeClientStart(
      ProviderCaller closeCaller, SnowflakeStreamingIngestClient client) {
    // needs new caller since client may be different
    ProviderCaller newCaller =
        new ProviderCaller(
            closeCaller.threadName,
            closeCaller.providerMethod,
            closeCaller.streamingClientProvider,
            client);

    newCaller.executeMethod();
    this.closeCallers.add(newCaller);

    return newCaller;
  }

  private void closeClientStop(List<ProviderCaller> callers) {
    callers.forEach(caller -> caller.closeClient());
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

    public SnowflakeStreamingIngestClient getClient() {
      this.joinMethod();
      return this.returnClient;
    }

    public void closeClient() {
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
