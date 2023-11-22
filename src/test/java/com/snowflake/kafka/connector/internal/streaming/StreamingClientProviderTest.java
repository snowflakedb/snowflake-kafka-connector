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

import static com.snowflake.kafka.connector.Utils.SF_ROLE;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProvider.StreamingClientProperties;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProvider.getStreamingClientProviderForTests;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.LoadingCache;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class StreamingClientProviderTest {
  // NOTE: use the following clients where possible so we don't leak clients - these will be closed
  // after each test
  private SnowflakeStreamingIngestClient client1;
  private SnowflakeStreamingIngestClient client2;
  private SnowflakeStreamingIngestClient client3;
  private SnowflakeStreamingIngestClient validClient;
  private SnowflakeStreamingIngestClient invalidClient;

  private Map<String, String> clientConfig1;
  private Map<String, String> clientConfig2;

  private StreamingClientProvider streamingClientProvider;
  private StreamingClientHandler streamingClientHandler;
  private boolean enableClientOptimization;

  @Parameterized.Parameters(name = "enableClientOptimization: {0}")
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
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        this.enableClientOptimization + "");
    this.clientConfig2 = new HashMap<>(this.clientConfig1);

    this.clientConfig1.put(Utils.NAME, "client1");
    this.clientConfig1.put(SF_ROLE, "testrole_kafka");
    this.clientConfig2.put(Utils.NAME, "client2");
    this.clientConfig2.put(SF_ROLE, "public");

    this.streamingClientHandler = Mockito.spy(StreamingClientHandler.class);
    this.streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            this.streamingClientHandler,
            StreamingClientProvider.buildLoadingCache(this.streamingClientHandler));
  }

  @After
  public void tearDown() {
    this.streamingClientHandler.closeClient(this.client1);
    this.streamingClientHandler.closeClient(this.client2);
    this.streamingClientHandler.closeClient(this.client3);
    this.streamingClientHandler.closeClient(this.validClient);
    this.streamingClientHandler.closeClient(this.invalidClient);
  }

  @Test
  public void testFirstGetClient() {
    // test actual provider
    this.client1 = this.streamingClientProvider.getClient(this.clientConfig1);

    // verify - should create a client regardless of optimization
    assert StreamingClientHandler.isClientValid(this.client1);
    assert this.client1.getName().contains(this.clientConfig1.get(Utils.NAME));
    Mockito.verify(this.streamingClientHandler, Mockito.times(1))
        .createClient(new StreamingClientProperties(this.clientConfig1));
  }

  @Test
  public void testGetInvalidClient() {
    Map<String, String> invalidClientConfig = new HashMap<>(this.clientConfig1);
    Map<String, String> validClientConfig = new HashMap<>(this.clientConfig2);

    // get valid and invalid client
    this.validClient = this.streamingClientProvider.getClient(validClientConfig);
    this.invalidClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(this.invalidClient.isClosed()).thenReturn(true);

    // inject new handler and cache
    StreamingClientHandler injectedStreamingClientHandler =
        Mockito.spy(StreamingClientHandler.class);
    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient>
        injectedRegistrationClients =
            StreamingClientProvider.buildLoadingCache(injectedStreamingClientHandler);
    injectedRegistrationClients.put(new StreamingClientProperties(validClientConfig), validClient);
    injectedRegistrationClients.put(
        new StreamingClientProperties(invalidClientConfig), invalidClient);

    StreamingClientProvider injectedProvider =
        getStreamingClientProviderForTests(
            injectedStreamingClientHandler, injectedRegistrationClients);

    // test: getting valid client
    this.validClient = injectedProvider.getClient(validClientConfig);

    // verify: valid client was got, but if optimization enabled we didnt need to create a new
    // client
    assert StreamingClientHandler.isClientValid(this.validClient);
    assert this.validClient.getName().contains(validClientConfig.get(Utils.NAME));
    assert !this.validClient.getName().contains(invalidClientConfig.get(Utils.NAME));
    Mockito.verify(
            injectedStreamingClientHandler, Mockito.times(this.enableClientOptimization ? 0 : 1))
        .createClient(new StreamingClientProperties(validClientConfig));

    // test: getting invalid client
    this.invalidClient = injectedProvider.getClient(invalidClientConfig);

    // verify: invalid client was refreshed / recreated
    assert StreamingClientHandler.isClientValid(this.invalidClient);
    assert !this.invalidClient.getName().contains(validClientConfig.get(Utils.NAME));
    assert this.invalidClient.getName().contains(invalidClientConfig.get(Utils.NAME));
    Mockito.verify(injectedStreamingClientHandler, Mockito.times(1))
        .createClient(new StreamingClientProperties(invalidClientConfig));
  }

  @Test
  public void testGetExistingClient() {
    // test creating client1 and client3 with the same config, client2 with different config
    this.client1 = this.streamingClientProvider.getClient(this.clientConfig1);
    this.client2 = this.streamingClientProvider.getClient(this.clientConfig2);
    this.client3 = this.streamingClientProvider.getClient(this.clientConfig1);

    // verify: clients are valid
    assert StreamingClientHandler.isClientValid(client1);
    assert StreamingClientHandler.isClientValid(client2);
    assert StreamingClientHandler.isClientValid(client3);

    // verify: client1 == client3 if optimization is enabled, but client2 should be different
    if (this.enableClientOptimization) {
      assert !client1.getName().equals(client2.getName());
      assert client1.getName().equals(client3.getName());
      assert client1.getName().contains(this.clientConfig1.get(Utils.NAME));

      Mockito.verify(this.streamingClientHandler, Mockito.times(1))
          .createClient(new StreamingClientProperties(this.clientConfig1));
      Mockito.verify(this.streamingClientHandler, Mockito.times(1))
          .createClient(new StreamingClientProperties(this.clientConfig2));
    } else {
      // client 1 and 3 are created from the same config, but will have different names
      assert !client1.getName().equals(client2.getName());
      assert !client2.getName().equals(client3.getName());
      assert !client1.getName().equals(client3.getName());

      assert client1.getName().contains(this.clientConfig1.get(Utils.NAME));
      assert client2.getName().contains(this.clientConfig2.get(Utils.NAME));
      assert client3.getName().contains(this.clientConfig1.get(Utils.NAME));

      Mockito.verify(this.streamingClientHandler, Mockito.times(2))
          .createClient(new StreamingClientProperties(this.clientConfig1));
      Mockito.verify(this.streamingClientHandler, Mockito.times(1))
          .createClient(new StreamingClientProperties(this.clientConfig2));
    }
  }

  @Test
  public void testCloseClients() throws Exception {
    // setup two valid clients
    this.client1 = this.streamingClientProvider.getClient(this.clientConfig1);
    this.client2 = this.streamingClientProvider.getClient(this.clientConfig2);
    assert StreamingClientHandler.isClientValid(this.client1);
    assert StreamingClientHandler.isClientValid(this.client2);

    // test closing valid client
    this.streamingClientProvider.closeClient(this.clientConfig1, this.client1);

    // verify: if optimized, there should only be one closeClient() call
    if (this.enableClientOptimization) {
      assert this.streamingClientProvider.getRegisteredClients().size() == 1; // just client 2 left
      Mockito.verify(this.streamingClientHandler, Mockito.times(2)).closeClient(this.client1);
    } else {
      assert this.streamingClientProvider.getRegisteredClients().size()
          == 0; // no registered clients without optimization
      Mockito.verify(this.streamingClientHandler, Mockito.times(1)).closeClient(this.client1);
    }
  }

  @Test
  public void testCloseInvalidClient() throws Exception {
    // inject invalid client
    this.invalidClient = this.streamingClientProvider.getClient(this.clientConfig1);
    this.invalidClient.close();

    // test closing invalid client
    this.streamingClientProvider.closeClient(this.clientConfig1, this.invalidClient);

    // close called twice with optimization, second should noop
    Mockito.verify(
            this.streamingClientHandler, Mockito.times(this.enableClientOptimization ? 2 : 1))
        .closeClient(this.invalidClient);
  }

  @Test
  public void testCloseUnregisteredClient() {
    // inject two clients
    this.client1 = this.streamingClientProvider.getClient(this.clientConfig1);
    this.client2 = this.streamingClientProvider.getClient(this.clientConfig2);

    // test somehow mixed up client1 and client2 config
    this.streamingClientProvider.closeClient(this.clientConfig1, this.client2);

    // verify both clients are closed with optimization, or just client2 without
    Mockito.verify(
            this.streamingClientHandler, Mockito.times(this.enableClientOptimization ? 1 : 0))
        .closeClient(this.client1);
    Mockito.verify(this.streamingClientHandler, Mockito.times(1)).closeClient(this.client2);
  }

  @Test
  public void testGetClientMissingConfig() {
    this.clientConfig1.remove(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG);

    // test actual provider
    this.client1 = this.streamingClientProvider.getClient(this.clientConfig1);
    this.client2 = this.streamingClientProvider.getClient(this.clientConfig1);

    // Since it is enabled by default, we should only create one client.
    assert this.client1.getName().equals(this.client2.getName());

    assert StreamingClientHandler.isClientValid(this.client1);
    assert this.client1.getName().contains(this.clientConfig1.get(Utils.NAME));
    Mockito.verify(this.streamingClientHandler, Mockito.times(1))
        .createClient(new StreamingClientProperties(this.clientConfig1));
  }
}
