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

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class StreamingClientProviderConcurrencyTest {
  private void testWithClient(RunnableWithException testFn, boolean enableClientOptimization)
      throws Exception {
    // set up the client
    Map<String, String> clientConfig = TestUtils.getConfForStreaming();
    clientConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        String.valueOf(enableClientOptimization));
    FakeStreamingClientHandler streamingClientHandler = new FakeStreamingClientHandler();
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            streamingClientHandler,
            StreamingClientProvider.buildLoadingCache(streamingClientHandler));

    // run the test itself
    testFn.run(streamingClientProvider, streamingClientHandler, clientConfig);
  }

  @Disabled
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  // SNOW-840882 flakey test
  public void testMultipleGetAndClose(boolean enableClientOptimization) throws Exception {
    testWithClient(
        (streamingClientProvider, streamingClientHandler, clientConfig) -> {
          // setup configs for 3 clients
          Map<String, String> clientConfig1 = new HashMap<>(clientConfig);
          Map<String, String> clientConfig2 = new HashMap<>(clientConfig);
          Map<String, String> clientConfig3 = new HashMap<>(clientConfig);

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

          getClient1Futures.add(
              this.callGetClientThread(
                  streamingClientProvider, task1Executor, task1Latch, clientConfig1));
          getClient1Futures.add(
              this.callGetClientThread(
                  streamingClientProvider, task1Executor, task1Latch, clientConfig1));
          getClient1Futures.add(
              this.callGetClientThread(
                  streamingClientProvider, task1Executor, task1Latch, clientConfig1));
          closeClient1Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
                  task1Executor,
                  task1Latch,
                  clientConfig1,
                  getClient1Futures.get(getClient1Futures.size() - 1).get()));
          getClient1Futures.add(
              this.callGetClientThread(
                  streamingClientProvider, task1Executor, task1Latch, clientConfig1));
          createClientCount++;
          closeClient1Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
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

          getClient2Futures.add(
              this.callGetClientThread(
                  streamingClientProvider, task2Executor, task2Latch, clientConfig1));
          closeClient2Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
                  task2Executor,
                  task2Latch,
                  clientConfig2,
                  getClient2Futures.get(getClient2Futures.size() - 1).get()));
          closeClient2Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
                  task2Executor,
                  task2Latch,
                  clientConfig2,
                  getClient2Futures.get(getClient2Futures.size() - 1).get()));
          closeClient2Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
                  task2Executor,
                  task2Latch,
                  clientConfig2,
                  getClient2Futures.get(getClient2Futures.size() - 1).get()));
          getClient2Futures.add(
              this.callGetClientThread(
                  streamingClientProvider, task2Executor, task2Latch, clientConfig1));
          createClientCount++;
          closeClient2Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
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

          getClient3Futures.add(
              this.callGetClientThread(
                  streamingClientProvider, task3Executor, task3Latch, clientConfig1));
          closeClient3Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
                  task3Executor,
                  task3Latch,
                  clientConfig3,
                  getClient3Futures.get(getClient3Futures.size() - 1).get()));

          // add final close to each task, kicking the threads off
          closeClient1Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
                  task1Executor,
                  task1Latch,
                  clientConfig1,
                  getClient1Futures.get(getClient1Futures.size() - 1).get()));
          closeClient2Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
                  task2Executor,
                  task2Latch,
                  clientConfig2,
                  getClient2Futures.get(getClient2Futures.size() - 1).get()));
          closeClient3Futures.add(
              this.callCloseClientThread(
                  streamingClientProvider,
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
                  streamingClientHandler,
                  Mockito.times(enableClientOptimization ? createClientCount : totalGetCount))
              .createClient(Mockito.any());
          Mockito.verify(streamingClientHandler, Mockito.times(totalCloseCount))
              .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));
        },
        enableClientOptimization);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testGetClientConcurrency(boolean enableClientOptimization) throws Exception {
    testWithClient(
        (streamingClientProvider, streamingClientHandler, clientConfig) -> {
          // setup getClient threads
          int numGetClientCalls = 10;
          CountDownLatch latch = new CountDownLatch(numGetClientCalls);
          ExecutorService executorService = Executors.newFixedThreadPool(numGetClientCalls);

          // start getClient threads
          List<Future<SnowflakeStreamingIngestClient>> futures = new ArrayList<>();
          for (int i = 0; i < numGetClientCalls; i++) {
            futures.add(
                this.callGetClientThread(
                    streamingClientProvider, executorService, latch, clientConfig));
          }

          // wait for getClient to complete
          latch.await();

          // Verify that clients are valid
          for (Future<SnowflakeStreamingIngestClient> future : futures) {
            Assert.assertTrue(StreamingClientHandler.isClientValid(future.get()));
          }

          // Verify that createClient() was called the expected number of times, once for enabled
          // param
          assertThat(streamingClientHandler.getCreateClientCalls())
              .isEqualTo(enableClientOptimization ? 1 : numGetClientCalls);
        },
        enableClientOptimization);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCloseClientConcurrency(boolean enableClientOptimization) throws Exception {
    testWithClient(
        (streamingClientProvider, streamingClientHandler, clientConfig) -> {
          int numCloseClientCalls = 10;
          SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(clientConfig);

          // setup closeClient threads
          CountDownLatch latch = new CountDownLatch(numCloseClientCalls);
          ExecutorService executorService = Executors.newFixedThreadPool(numCloseClientCalls);

          // start closeClient threads
          List<Future<SnowflakeStreamingIngestClient>> futures = new ArrayList<>();
          for (int i = 0; i < numCloseClientCalls; i++) {
            futures.add(
                this.callCloseClientThread(
                    streamingClientProvider, executorService, latch, clientConfig, client));
          }

          // wait for closeClient to complete
          latch.await();

          // Verify that clients are invalid (closed)
          for (Future<SnowflakeStreamingIngestClient> future : futures) {
            Assert.assertFalse(StreamingClientHandler.isClientValid(future.get()));
          }

          // Verify that closeClient() at least once per thread
          assertThat(streamingClientHandler.getCloseClientCalls())
              .isGreaterThanOrEqualTo(numCloseClientCalls);

          // Verify that closeClient() was called at max twice per close thread. Because
          // LoadingCache's
          // invalidation happens async, we can't really expect an exact number of calls. The extra
          // close
          // client calls will no-op
          assertThat(streamingClientHandler.getCloseClientCalls())
              .isLessThanOrEqualTo(numCloseClientCalls * (enableClientOptimization ? 2 : 1));
        },
        enableClientOptimization);
  }

  private Future<SnowflakeStreamingIngestClient> callGetClientThread(
      StreamingClientProvider streamingClientProvider,
      ExecutorService executorService,
      CountDownLatch countDownLatch,
      Map<String, String> config) {
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
      StreamingClientProvider streamingClientProvider,
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

  private static interface RunnableWithException {
    void run(
        StreamingClientProvider streamingClientProvider,
        FakeStreamingClientHandler streamingClientHandler,
        Map<String, String> clientConfig)
        throws Exception;
  }
}
