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
import java.util.stream.Collectors;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.LoadingCache;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class StreamingClientProviderIT {
  private final boolean enableClientOptimization;
  private final Map<String, String> clientConfig = TestUtils.getConfForStreaming();

  @Parameterized.Parameters(name = "enableClientOptimization: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  public StreamingClientProviderIT(boolean enableClientOptimization) {
    this.enableClientOptimization = enableClientOptimization;
    this.clientConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        String.valueOf(this.enableClientOptimization));
  }

  @Test
  public void testGetMultipleClients() throws Exception {
    String validRegisteredClientName = "openRegisteredClient";
    String invalidRegisteredClientName = "closedRegisteredClient";
    String validUnregisteredClientName = "openUnregisteredClient";
    StreamingClientHandler clientCreator = new StreamingClientHandler();

    // setup registered valid client
    Map<String, String> validRegisteredClientConfig = new HashMap<>(this.clientConfig);
    validRegisteredClientConfig.put(Utils.NAME, validRegisteredClientName);
    validRegisteredClientConfig.put(Utils.SF_OAUTH_CLIENT_ID, "0");
    StreamingClientProperties validRegisteredClientProps =
        new StreamingClientProperties(validRegisteredClientConfig);
    SnowflakeStreamingIngestClient validRegisteredClient =
        clientCreator.createClient(validRegisteredClientProps);

    // setup registered invalid client
    Map<String, String> invalidRegisteredClientConfig = new HashMap<>(this.clientConfig);
    invalidRegisteredClientConfig.put(Utils.NAME, invalidRegisteredClientName);
    invalidRegisteredClientConfig.put(Utils.SF_OAUTH_CLIENT_ID, "1");
    StreamingClientProperties invalidRegisteredClientProps =
        new StreamingClientProperties(invalidRegisteredClientConfig);
    SnowflakeStreamingIngestClient invalidRegisteredClient =
        clientCreator.createClient(invalidRegisteredClientProps);
    invalidRegisteredClient.close();

    // setup unregistered valid client
    Map<String, String> validUnregisteredClientConfig = new HashMap<>(this.clientConfig);
    validUnregisteredClientConfig.put(Utils.NAME, validUnregisteredClientName);
    validUnregisteredClientConfig.put(Utils.SF_OAUTH_CLIENT_ID, "2");
    StreamingClientProperties validUnregisteredClientProps =
        new StreamingClientProperties(validUnregisteredClientConfig);
    SnowflakeStreamingIngestClient validUnregisteredClient =
        clientCreator.createClient(validUnregisteredClientProps);

    // inject registered clients
    StreamingClientHandler streamingClientHandlerSpy =
        Mockito.spy(StreamingClientHandler.class); // use this to verify behavior
    LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> registeredClients =
        StreamingClientProvider.buildLoadingCache(streamingClientHandlerSpy);

    registeredClients.put(validRegisteredClientProps, validRegisteredClient);
    registeredClients.put(invalidRegisteredClientProps, invalidRegisteredClient);

    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderForTests(
            streamingClientHandlerSpy, registeredClients);

    assert streamingClientProvider.getRegisteredClients().size() == 2;

    // test 1: get registered valid client optimization returns existing client
    SnowflakeStreamingIngestClient resultValidRegisteredClient =
        streamingClientProvider.getClient(validRegisteredClientConfig);

    assert StreamingClientHandler.isClientValid(resultValidRegisteredClient);
    assert resultValidRegisteredClient.getName().contains("_0");
    assert this.enableClientOptimization
        == resultValidRegisteredClient.equals(validRegisteredClient);
    Mockito.verify(streamingClientHandlerSpy, Mockito.times(this.enableClientOptimization ? 0 : 1))
        .createClient(validRegisteredClientProps);
    assert streamingClientProvider.getRegisteredClients().size() == 2;

    // test 2: get registered invalid client creates new client regardless of optimization
    SnowflakeStreamingIngestClient resultInvalidRegisteredClient =
        streamingClientProvider.getClient(invalidRegisteredClientConfig);

    assert StreamingClientHandler.isClientValid(resultInvalidRegisteredClient);
    assert resultInvalidRegisteredClient
        .getName()
        .contains("_" + (this.enableClientOptimization ? 0 : 1));
    assert !resultInvalidRegisteredClient.equals(invalidRegisteredClient);
    Mockito.verify(streamingClientHandlerSpy, Mockito.times(1))
        .createClient(invalidRegisteredClientProps);
    assert streamingClientProvider.getRegisteredClients().size() == 2;

    // test 3: get unregistered valid client creates and registers new client with optimization
    SnowflakeStreamingIngestClient resultValidUnregisteredClient =
        streamingClientProvider.getClient(validUnregisteredClientConfig);

    assert StreamingClientHandler.isClientValid(resultValidUnregisteredClient);
    assert resultValidUnregisteredClient
        .getName()
        .contains("_" + (this.enableClientOptimization ? 1 : 2));
    assert !resultValidUnregisteredClient.equals(validUnregisteredClient);
    Mockito.verify(streamingClientHandlerSpy, Mockito.times(1))
        .createClient(validUnregisteredClientProps);
    assert streamingClientProvider.getRegisteredClients().size()
        == (this.enableClientOptimization ? 3 : 2);

    // verify streamingClientHandler behavior
    Mockito.verify(streamingClientHandlerSpy, Mockito.times(this.enableClientOptimization ? 2 : 3))
        .createClient(Mockito.any());

    // test 4: get all clients multiple times and verify optimization doesn't create new clients
    List<SnowflakeStreamingIngestClient> gotClientList = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      gotClientList.add(streamingClientProvider.getClient(validRegisteredClientConfig));
      gotClientList.add(streamingClientProvider.getClient(invalidRegisteredClientConfig));
      gotClientList.add(streamingClientProvider.getClient(validUnregisteredClientConfig));
    }

    List<SnowflakeStreamingIngestClient> distinctClients =
        gotClientList.stream().distinct().collect(Collectors.toList());
    assert distinctClients.size() == (this.enableClientOptimization ? 3 : gotClientList.size());
    Mockito.verify(
            streamingClientHandlerSpy,
            Mockito.times(this.enableClientOptimization ? 2 : 3 + gotClientList.size()))
        .createClient(Mockito.any());
    assert streamingClientProvider.getRegisteredClients().size()
        == (this.enableClientOptimization ? 3 : 2);

    // close all clients
    validRegisteredClient.close();
    invalidRegisteredClient.close();
    validUnregisteredClient.close();

    resultValidRegisteredClient.close();
    resultInvalidRegisteredClient.close();
    resultValidUnregisteredClient.close();

    distinctClients.stream()
        .forEach(
            client -> {
              try {
                client.close();
              } catch (Exception e) {
                // do nothing
              }
            });
  }
}
