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
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class StreamingClientProviderIT {

  @Test
  @Disabled("CI flaky")
  public void getClient_forOptimizationEnabled_returnSameClient() {
    // given
    Map<String, String> clientConfig = getClientConfig(true);
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderInstance();

    // when
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(clientConfig);
    SnowflakeStreamingIngestClient client2 = streamingClientProvider.getClient(clientConfig);

    // then
    Assertions.assertSame(client, client2);

    int clientsRegistered = streamingClientProvider.getRegisteredClients().size();
    Assertions.assertEquals(1, clientsRegistered);
  }

  @Test
  public void getClient_forOptimizationDisabled_returnDifferentClients() {
    // given
    Map<String, String> clientConfig = getClientConfig(false);
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderInstance();

    // when
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(clientConfig);
    SnowflakeStreamingIngestClient client2 = streamingClientProvider.getClient(clientConfig);

    // then
    Assertions.assertNotNull(client);
    Assertions.assertNotNull(client2);
    Assertions.assertNotEquals(client, client2);
  }

  @Test
  public void getClient_forInvalidClient_returnNewInstance() throws Exception {
    // given
    Map<String, String> clientConfig = getClientConfig(true);
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderInstance();

    // when
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(clientConfig);
    client.close();
    SnowflakeStreamingIngestClient client2 = streamingClientProvider.getClient(clientConfig);

    // then
    Assertions.assertTrue(client.isClosed());
    Assertions.assertFalse(client2.isClosed());
  }

  @Test
  public void getClient_forMissingOptimizationConfig_returnValidClient() {
    // given
    Map<String, String> clientConfig = getClientConfig(true);
    clientConfig.remove(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG);
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderInstance();

    // when
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(clientConfig);

    // then
    Assertions.assertNotNull(client);
    Assertions.assertFalse(client.isClosed());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void closeClient_forValidClient_stopTheClient(boolean clientOptimizationEnabled) {
    // given
    Map<String, String> clientConfig = getClientConfig(clientOptimizationEnabled);
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderInstance();

    // when
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(clientConfig);
    streamingClientProvider.closeClient(clientConfig, client);

    // then
    Assertions.assertTrue(client.isClosed());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void closeClient_forAlreadyStoppedClient_doesNotThrowException(
      boolean clientOptimizationEnabled) {
    // given
    Map<String, String> clientConfig = getClientConfig(clientOptimizationEnabled);
    StreamingClientProvider streamingClientProvider =
        StreamingClientProvider.getStreamingClientProviderInstance();

    // when
    SnowflakeStreamingIngestClient client = streamingClientProvider.getClient(clientConfig);
    streamingClientProvider.closeClient(clientConfig, client);

    // then
    Assertions.assertDoesNotThrow(() -> streamingClientProvider.closeClient(clientConfig, client));
    Assertions.assertTrue(client.isClosed());
  }

  private Map<String, String> getClientConfig(boolean clientOptimizationEnabled) {
    Map<String, String> clientConfig = TestUtils.getConfForStreaming();
    clientConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        String.valueOf(clientOptimizationEnabled));
    return clientConfig;
  }
}
