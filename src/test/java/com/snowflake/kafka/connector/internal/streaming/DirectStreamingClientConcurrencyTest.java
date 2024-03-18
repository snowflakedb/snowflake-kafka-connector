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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class DirectStreamingClientConcurrencyTest {
  private Map<String, String> clientConfig;

  private StreamingClientProvider streamingClientProvider;
  private StreamingClientHandler streamingClientHandler;
  private boolean enableClientOptimization;

  private List<Future<SnowflakeStreamingIngestClient>> getClientFuturesTeardown;
  private List<Future> closeClientFuturesTeardown;

  @Parameterized.Parameters(name = "enableClientOptimization: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  public DirectStreamingClientConcurrencyTest(boolean enableClientOptimization) {
    this.enableClientOptimization = enableClientOptimization;
  }

  @Before
  public void setup() {
    this.clientConfig = TestUtils.getConfForStreaming();
    this.clientConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        this.enableClientOptimization + "");

    this.streamingClientHandler = Mockito.spy(DirectStreamingClientHandler.class);
    this.streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            this.streamingClientHandler,
            StreamingClientProvider.buildLoadingCache(this.streamingClientHandler));

    this.getClientFuturesTeardown = new ArrayList<>();
    this.closeClientFuturesTeardown = new ArrayList<>();
  }

  @After
  public void tearDown() throws Exception {
    // clean up all the threads
    try {
      for (Future<SnowflakeStreamingIngestClient> getClientFuture : this.getClientFuturesTeardown) {
        getClientFuture.get().close();
      }
      for (Future closeClientFuture : this.closeClientFuturesTeardown) {
        closeClientFuture.get();
      }
    } catch (Exception ex) {
      throw ex;
    }
  }

  @Ignore
  // SNOW-840882 flakey test
  public void testMultipleGetAndClose() throws Exception {
    // setup configs for 3 clients
    Map<String, String> clientConfig1 = new HashMap<>(this.clientConfig);
    Map<String, String> clientConfig2 = new HashMap<>(this.clientConfig);
    Map<String, String> clientConfig3 = new HashMap<>(this.clientConfig);

    clientConfig1.put(Utils.NAME, "client1");
    clientConfig2.put(Utils.NAME, "client2");
    clientConfig3.put(Utils.NAME, "client3");

    int createClientCount = 0;

    // task1: get client x3, close client, get client, close client
    CountDownLatch task1Latch = new CountDownLatch(7);
    ExecutorService task1Executor = Executors.newSingleThreadExecutor();
    List<Future<SnowflakeStreamingIngestClient>> getClient1Futures = new ArrayList<>();
    List<Future> closeClient1Futures = new ArrayList<>();
    createClientCount++;

    getClient1Futures.add(this.callGetClientThread(task1Executor, task1Latch, clientConfig1));
    getClient1Futures.add(this.callGetClientThread(task1Executor, task1Latch, clientConfig1));
    getClient1Futures.add(this.callGetClientThread(task1Executor, task1Latch, clientConfig1));
    closeClient1Futures.add(
        this.callCloseClientThread(
            task1Executor,
            task1Latch,
            clientConfig1,
            getClient1Futures.get(getClient1Futures.size() - 1).get()));
    getClient1Futures.add(this.callGetClientThread(task1Executor, task1Latch, clientConfig1));
    createClientCount++;
    closeClient1Futures.add(
        this.callCloseClientThread(
            task1Executor,
            task1Latch,
            clientConfig1,
            getClient1Futures.get(getClient1Futures.size() - 1).get()));

    // task2: get client, close client x3, get client, close client
    CountDownLatch task2Latch = new CountDownLatch(7);
    ExecutorService task2Executor = Executors.newSingleThreadExecutor();
    List<Future<SnowflakeStreamingIngestClient>> getClient2Futures = new ArrayList<>();
    List<Future> closeClient2Futures = new ArrayList<>();
    createClientCount++;

    getClient2Futures.add(this.callGetClientThread(task2Executor, task2Latch, clientConfig1));
    closeClient2Futures.add(
        this.callCloseClientThread(
            task2Executor,
            task2Latch,
            clientConfig2,
            getClient2Futures.get(getClient2Futures.size() - 1).get()));
    closeClient2Futures.add(
        this.callCloseClientThread(
            task2Executor,
            task2Latch,
            clientConfig2,
            getClient2Futures.get(getClient2Futures.size() - 1).get()));
    closeClient2Futures.add(
        this.callCloseClientThread(
            task2Executor,
            task2Latch,
            clientConfig2,
            getClient2Futures.get(getClient2Futures.size() - 1).get()));
    getClient2Futures.add(this.callGetClientThread(task2Executor, task2Latch, clientConfig1));
    createClientCount++;
    closeClient2Futures.add(
        this.callCloseClientThread(
            task2Executor,
            task2Latch,
            clientConfig2,
            getClient2Futures.get(getClient2Futures.size() - 1).get()));

    // task3: get client, close client
    CountDownLatch task3Latch = new CountDownLatch(3);
    ExecutorService task3Executor = Executors.newSingleThreadExecutor();
    List<Future<SnowflakeStreamingIngestClient>> getClient3Futures = new ArrayList<>();
    List<Future> closeClient3Futures = new ArrayList<>();
    createClientCount++;

    getClient3Futures.add(this.callGetClientThread(task3Executor, task3Latch, clientConfig1));
    closeClient3Futures.add(
        this.callCloseClientThread(
            task3Executor,
            task3Latch,
            clientConfig3,
            getClient3Futures.get(getClient3Futures.size() - 1).get()));

    // add final close to each task, kicking the threads off
    closeClient1Futures.add(
        this.callCloseClientThread(
            task1Executor,
            task1Latch,
            clientConfig1,
            getClient1Futures.get(getClient1Futures.size() - 1).get()));
    closeClient2Futures.add(
        this.callCloseClientThread(
            task2Executor,
            task2Latch,
            clientConfig2,
            getClient2Futures.get(getClient2Futures.size() - 1).get()));
    closeClient3Futures.add(
        this.callCloseClientThread(
            task3Executor,
            task3Latch,
            clientConfig3,
            getClient3Futures.get(getClient3Futures.size() - 1).get()));

    task1Latch.await();
    task2Latch.await();
    task3Latch.await();

    // verify createClient and closeClient calls
    int totalCloseCount =
        closeClient1Futures.size() + closeClient2Futures.size() + closeClient3Futures.size();
    int totalGetCount =
        getClient1Futures.size() + getClient2Futures.size() + getClient3Futures.size();

    Mockito.verify(
            this.streamingClientHandler,
            Mockito.times(this.enableClientOptimization ? createClientCount : totalGetCount))
        .createClient(Mockito.any());
    Mockito.verify(this.streamingClientHandler, Mockito.times(totalCloseCount))
        .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));
  }

  @Test
  public void testGetClientConcurrency() throws Exception {
    // setup getClient threads
    int numGetClientCalls = 10;
    CountDownLatch latch = new CountDownLatch(numGetClientCalls);
    ExecutorService executorService = Executors.newFixedThreadPool(numGetClientCalls);

    // start getClient threads
    List<Future<SnowflakeStreamingIngestClient>> futures = new ArrayList<>();
    for (int i = 0; i < numGetClientCalls; i++) {
      futures.add(this.callGetClientThread(executorService, latch, this.clientConfig));
    }

    // wait for getClient to complete
    latch.await();

    // Verify that clients are valid
    for (Future<SnowflakeStreamingIngestClient> future : futures) {
      Assert.assertTrue(StreamingClientHandler.isClientValid(future.get()));
    }

    // Verify that createClient() was called the expected number of times, once for enabled param
    Mockito.verify(
            this.streamingClientHandler,
            Mockito.times(this.enableClientOptimization ? 1 : numGetClientCalls))
        .createClient(Mockito.any());
  }

  @Test
  public void testCloseClientConcurrency() throws Exception {
    int numCloseClientCalls = 10;
    SnowflakeStreamingIngestClient client =
        this.streamingClientProvider.getClient(this.clientConfig);

    // setup closeClient threads
    CountDownLatch latch = new CountDownLatch(numCloseClientCalls);
    ExecutorService executorService = Executors.newFixedThreadPool(numCloseClientCalls);

    // start closeClient threads
    List<Future<SnowflakeStreamingIngestClient>> futures = new ArrayList<>();
    for (int i = 0; i < numCloseClientCalls; i++) {
      futures.add(this.callCloseClientThread(executorService, latch, clientConfig, client));
    }

    // wait for closeClient to complete
    latch.await();

    // Verify that clients are invalid (closed)
    for (Future<SnowflakeStreamingIngestClient> future : futures) {
      Assert.assertFalse(StreamingClientHandler.isClientValid(future.get()));
    }

    // Verify that closeClient() at least once per thread
    Mockito.verify(this.streamingClientHandler, Mockito.atLeast(numCloseClientCalls))
        .closeClient(client);

    // Verify that closeClient() was called at max twice per close thread. Because LoadingCache's
    // invalidation happens async, we can't really expect an exact number of calls. The extra close
    // client calls will no-op
    Mockito.verify(
            this.streamingClientHandler,
            Mockito.atMost(numCloseClientCalls * (this.enableClientOptimization ? 2 : 1)))
        .closeClient(client);
  }

  private Future<SnowflakeStreamingIngestClient> callGetClientThread(
      ExecutorService executorService, CountDownLatch countDownLatch, Map<String, String> config) {
    Future<SnowflakeStreamingIngestClient> future =
        executorService.submit(
            () -> {
              SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(config);
              countDownLatch.countDown();
              return client;
            });

    return future;
  }

  private Future callCloseClientThread(
      ExecutorService executorService,
      CountDownLatch countDownLatch,
      Map<String, String> config,
      SnowflakeStreamingIngestClient client) {
    Future future =
        executorService.submit(
            () -> {
              streamingClientProvider.closeClient(config, client);
              countDownLatch.countDown();
            });

    return future;
  }
}
