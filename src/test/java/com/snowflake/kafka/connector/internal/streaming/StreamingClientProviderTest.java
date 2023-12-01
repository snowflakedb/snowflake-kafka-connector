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
import java.util.Map;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.LoadingCache;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class StreamingClientProviderTest {
  private final boolean enableClientOptimization;
  private final Map<String, String> clientConfig = TestUtils.getConfForStreaming();

  @Parameterized.Parameters(name = "enableClientOptimization: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  public StreamingClientProviderTest(boolean enableClientOptimization) {
    this.enableClientOptimization = enableClientOptimization;
    this.clientConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        String.valueOf(this.enableClientOptimization));
  }

  @Test
  public void testFirstGetClient() {
    // setup mock client and handler
    SnowflakeStreamingIngestClient clientMock = Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(clientMock.getName()).thenReturn(this.clientConfig.get(Utils.NAME));

    StreamingClientHandler mockClientHandler = Mockito.mock(StreamingClientHandler.class);
    Mockito.when(mockClientHandler.createClient(new StreamingClientProperties(this.clientConfig)))
        .thenReturn(clientMock);

    // test provider gets new client
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            mockClientHandler, StreamingClientProvider.buildLoadingCache(mockClientHandler));
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(this.clientConfig);

    // verify - should create a client regardless of optimization
    assert client.equals(clientMock);
    assert client.getName().contains(this.clientConfig.get(Utils.NAME));
    Mockito.verify(mockClientHandler, Mockito.times(1))
        .createClient(new StreamingClientProperties(this.clientConfig));
  }

  @Test
  public void testGetInvalidClient() {
    // setup handler, invalid mock client and valid returned client
    SnowflakeStreamingIngestClient mockInvalidClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(mockInvalidClient.getName()).thenReturn(this.clientConfig.get(Utils.NAME));
    Mockito.when(mockInvalidClient.isClosed()).thenReturn(true);

    SnowflakeStreamingIngestClient mockValidClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(mockValidClient.getName()).thenReturn(this.clientConfig.get(Utils.NAME));
    Mockito.when(mockValidClient.isClosed()).thenReturn(false);

    StreamingClientHandler mockClientHandler = Mockito.mock(StreamingClientHandler.class);
    Mockito.when(mockClientHandler.createClient(new StreamingClientProperties(this.clientConfig)))
        .thenReturn(mockValidClient);

    // inject invalid client into provider
    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> mockRegisteredClients =
        Mockito.mock(LoadingCache.class);
    Mockito.when(mockRegisteredClients.get(new StreamingClientProperties(this.clientConfig)))
        .thenReturn(mockInvalidClient);

    // test provider gets new client
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            mockClientHandler, mockRegisteredClients);
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(this.clientConfig);

    // verify - returned client is valid even though we injected an invalid client
    assert client.equals(mockValidClient);
    assert !client.equals(mockInvalidClient);
    assert client.getName().contains(this.clientConfig.get(Utils.NAME));
    assert !client.isClosed();
    Mockito.verify(mockClientHandler, Mockito.times(1))
        .createClient(new StreamingClientProperties(this.clientConfig));
  }

  @Test
  public void testGetExistingClient() {
    // setup existing client, handler and inject to registeredClients
    SnowflakeStreamingIngestClient mockExistingClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(mockExistingClient.getName()).thenReturn(this.clientConfig.get(Utils.NAME));
    Mockito.when(mockExistingClient.isClosed()).thenReturn(false);

    StreamingClientHandler mockClientHandler = Mockito.mock(StreamingClientHandler.class);

    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> mockRegisteredClients =
        Mockito.mock(LoadingCache.class);
    Mockito.when(mockRegisteredClients.get(new StreamingClientProperties(this.clientConfig)))
        .thenReturn(mockExistingClient);

    // if optimization is disabled, we will create new client regardless of registeredClientws
    if (!this.enableClientOptimization) {
      Mockito.when(mockClientHandler.createClient(new StreamingClientProperties(this.clientConfig)))
          .thenReturn(mockExistingClient);
    }

    // test getting client
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            mockClientHandler, mockRegisteredClients);
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(this.clientConfig);

    // verify client and expected client creation
    assert client.equals(mockExistingClient);
    assert client.getName().equals(this.clientConfig.get(Utils.NAME));
    Mockito.verify(mockClientHandler, Mockito.times(this.enableClientOptimization ? 0 : 1))
        .createClient(new StreamingClientProperties(this.clientConfig));
  }

  @Test
  public void testGetClientMissingConfig() {
    // remove one client opt from config
    this.clientConfig.remove(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG);

    // setup existing client, handler and inject to registeredClients
    SnowflakeStreamingIngestClient mockExistingClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(mockExistingClient.getName()).thenReturn(this.clientConfig.get(Utils.NAME));
    Mockito.when(mockExistingClient.isClosed()).thenReturn(false);

    StreamingClientHandler mockClientHandler = Mockito.mock(StreamingClientHandler.class);

    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> mockRegisteredClients =
        Mockito.mock(LoadingCache.class);
    Mockito.when(mockRegisteredClients.get(new StreamingClientProperties(this.clientConfig)))
        .thenReturn(mockExistingClient);

    // test getting client
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            mockClientHandler, mockRegisteredClients);
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(this.clientConfig);

    // verify returned existing client since removing the optimization should default to true
    assert client.equals(mockExistingClient);
    assert client.getName().equals(this.clientConfig.get(Utils.NAME));
    Mockito.verify(mockClientHandler, Mockito.times(0))
        .createClient(new StreamingClientProperties(this.clientConfig));
  }

  @Test
  public void testCloseClients() throws Exception {
    // setup valid existing client and handler
    SnowflakeStreamingIngestClient mockExistingClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(mockExistingClient.getName()).thenReturn(this.clientConfig.get(Utils.NAME));
    Mockito.when(mockExistingClient.isClosed()).thenReturn(false);

    StreamingClientHandler mockClientHandler = Mockito.mock(StreamingClientHandler.class);
    Mockito.doCallRealMethod()
        .when(mockClientHandler)
        .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));

    // inject existing client in for optimization
    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> mockRegisteredClients =
        Mockito.mock(LoadingCache.class);
    if (this.enableClientOptimization) {
      Mockito.when(
              mockRegisteredClients.getIfPresent(new StreamingClientProperties(this.clientConfig)))
          .thenReturn(mockExistingClient);
    }

    // test closing valid client
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            mockClientHandler, mockRegisteredClients);
    streamingClientProvider.closeClient(this.clientConfig, mockExistingClient);

    // verify existing client was closed, optimization will call given client and registered client
    Mockito.verify(mockClientHandler, Mockito.times(this.enableClientOptimization ? 2 : 1))
        .closeClient(mockExistingClient);
    Mockito.verify(mockExistingClient, Mockito.times(this.enableClientOptimization ? 2 : 1))
        .close();
  }

  @Test
  public void testCloseInvalidClient() throws Exception {
    // setup invalid existing client and handler
    SnowflakeStreamingIngestClient mockInvalidClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(mockInvalidClient.getName()).thenReturn(this.clientConfig.get(Utils.NAME));
    Mockito.when(mockInvalidClient.isClosed()).thenReturn(true);

    StreamingClientHandler mockClientHandler = Mockito.mock(StreamingClientHandler.class);
    Mockito.doCallRealMethod()
        .when(mockClientHandler)
        .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));

    // inject invalid existing client in for optimization
    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> mockRegisteredClients =
        Mockito.mock(LoadingCache.class);
    if (this.enableClientOptimization) {
      Mockito.when(
              mockRegisteredClients.getIfPresent(new StreamingClientProperties(this.clientConfig)))
          .thenReturn(mockInvalidClient);
    }

    // test closing valid client
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            mockClientHandler, mockRegisteredClients);
    streamingClientProvider.closeClient(this.clientConfig, mockInvalidClient);

    // verify handler close client no-op and client did not need to call close
    Mockito.verify(mockClientHandler, Mockito.times(this.enableClientOptimization ? 2 : 1))
        .closeClient(mockInvalidClient);
    Mockito.verify(mockInvalidClient, Mockito.times(0)).close();
  }

  @Test
  public void testCloseUnregisteredClient() throws Exception {
    // setup valid existing client and handler
    SnowflakeStreamingIngestClient mockUnregisteredClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(mockUnregisteredClient.getName()).thenReturn(this.clientConfig.get(Utils.NAME));
    Mockito.when(mockUnregisteredClient.isClosed()).thenReturn(false);

    StreamingClientHandler mockClientHandler = Mockito.mock(StreamingClientHandler.class);
    Mockito.doCallRealMethod()
        .when(mockClientHandler)
        .closeClient(Mockito.any(SnowflakeStreamingIngestClient.class));

    // ensure no clients are registered
    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> mockRegisteredClients =
        Mockito.mock(LoadingCache.class);
    Mockito.when(
            mockRegisteredClients.getIfPresent(new StreamingClientProperties(this.clientConfig)))
        .thenReturn(null);

    // test closing valid client
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            mockClientHandler, mockRegisteredClients);
    streamingClientProvider.closeClient(this.clientConfig, mockUnregisteredClient);

    // verify unregistered client was closed
    Mockito.verify(mockClientHandler, Mockito.times(1)).closeClient(mockUnregisteredClient);
    Mockito.verify(mockUnregisteredClient, Mockito.times(1)).close();
  }
}
