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

import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProvider.injectStreamingClientProviderForTests;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProvider.isClientValid;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

public class StreamingClientProviderTest {
  StreamingClientProvider streamingClientProvider = StreamingClientProvider.getStreamingClientProviderInstance();

  @After
  public void cleanUpProviderClient() {
    // note that the config will not be cleaned up
    streamingClientProvider.closeClient();
  }

  @Test
  public void testCreateAndGetClient() {
    // setup
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    String connectorName = connectorConfig.get(Utils.NAME);

    // test actual provider
    streamingClientProvider.createOrReplaceClient(connectorConfig);
    SnowflakeStreamingIngestClient createdClient =
        streamingClientProvider.getClient(connectorConfig);

    // verify
    assert createdClient.getName().contains(connectorName);
    assert StreamingClientProvider.isClientValid(createdClient);
  }

  @Test
  public void testReplaceAndGetClient() {
    String connector1 = "connector1";
    String connector2 = "connector2";

    // inject an existing client
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(Utils.NAME, connector1);
    SnowflakeStreamingIngestClient streamingIngestClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    StreamingClientProvider injectedProvider =
        injectStreamingClientProviderForTests(1, connectorConfig, streamingIngestClient);

    // test creating another client
    connectorConfig.put(Utils.NAME, connector2);
    injectedProvider.createOrReplaceClient(connectorConfig);
    SnowflakeStreamingIngestClient replacedClient = injectedProvider.getClient(connectorConfig);

    // verify
    assert !replacedClient.getName().contains(connector1);
    assert replacedClient.getName().contains(connector2);
    assert StreamingClientProvider.isClientValid(replacedClient);
  }

  @Test
  public void testOverrideClientBdecVersion() {
    // not really a great way to verify this works unfortunately
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION, "1");
    streamingClientProvider.createOrReplaceClient(connectorConfig);
  }

  @Test
  public void testCreateClientFailure() {
    try {
      // create client with empty config
      streamingClientProvider.createOrReplaceClient(new HashMap<>());
    } catch (ConnectException ex) {
      assert ex.getCause().getClass().equals(SFException.class);
    }
  }

  @Test
  public void testGetInvalidClient() {
    String invalidClientName = "invalid client";
    String validClientName = "valid client";

    // inject invalid client
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(Utils.NAME, invalidClientName);
    SnowflakeStreamingIngestClient invalidClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(invalidClient.isClosed()).thenReturn(true);
    StreamingClientProvider injectedProvider =
        injectStreamingClientProviderForTests(1, connectorConfig, invalidClient);

    // try getting client
    connectorConfig.put(Utils.NAME, validClientName);
    SnowflakeStreamingIngestClient recreatedClient = injectedProvider.getClient(connectorConfig);

    // verify this client is valid
    assert isClientValid(recreatedClient);
    assert recreatedClient.getName().contains(validClientName);
    assert !recreatedClient.getName().contains(invalidClientName);
  }

  @Test
  public void testGetClientWithDisabledParam() {
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "false");

    // all of these clients should be valid and have different names
    SnowflakeStreamingIngestClient client1 = streamingClientProvider.getClient(connectorConfig);
    SnowflakeStreamingIngestClient client2 = streamingClientProvider.getClient(connectorConfig);
    SnowflakeStreamingIngestClient client3 = streamingClientProvider.getClient(connectorConfig);

    // verify
    assert isClientValid(client1);
    assert isClientValid(client2);
    assert isClientValid(client3);

    assert !client1.getName().equals(client2.getName());
    assert !client2.getName().equals(client3.getName());
    assert !client1.getName().equals(client3.getName());
  }

  @Test
  public void testGetClientWithEnabledParam() {
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "true");

    // all of these clients should be valid and have the names
    SnowflakeStreamingIngestClient client1 = streamingClientProvider.getClient(connectorConfig);
    SnowflakeStreamingIngestClient client2 = streamingClientProvider.getClient(connectorConfig);
    SnowflakeStreamingIngestClient client3 = streamingClientProvider.getClient(connectorConfig);

    // verify
    assert isClientValid(client1);
    assert isClientValid(client2);
    assert isClientValid(client3);

    assert client1.getName().equals(client2.getName());
    assert client2.getName().equals(client3.getName());
    assert client1.getName().equals(client3.getName());
  }

  @Test
  public void testCloseClient() throws Exception {
    // inject an existing client
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    SnowflakeStreamingIngestClient streamingIngestClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(streamingIngestClient.isClosed()).thenReturn(false);
    Mockito.when(streamingIngestClient.getName()).thenReturn("testclient");
    StreamingClientProvider injectedProvider =
        injectStreamingClientProviderForTests(1, connectorConfig, streamingIngestClient);

    // try closing client
    injectedProvider.closeClient();

    // verify closed
    Mockito.verify(streamingIngestClient, Mockito.times(1)).close();
    Mockito.verify(streamingIngestClient, Mockito.times(1)).isClosed();
    Mockito.verify(streamingIngestClient, Mockito.times(2)).getName();
  }

  @Test
  public void testCloseInvalidClient() throws Exception {
    // inject invalid client
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    SnowflakeStreamingIngestClient streamingIngestClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(streamingIngestClient.isClosed()).thenReturn(true);
    StreamingClientProvider injectedProvider =
        injectStreamingClientProviderForTests(1, connectorConfig, streamingIngestClient);

    // try closing client
    injectedProvider.closeClient();

    // verify didn't call close
    Mockito.verify(streamingIngestClient, Mockito.times(0)).close();
  }

  @Test
  public void testCloseClientWithVariousExceptions() throws Exception {
    List<Exception> exceptionsToTest = new ArrayList<>();

    Exception nullMessageEx = new Exception();
    exceptionsToTest.add(nullMessageEx);

    Exception nullCauseEx = new Exception("nullCauseEx");
    nullCauseEx.initCause(null);
    exceptionsToTest.add(nullCauseEx);

    Exception stacktraceEx = new Exception("stacktraceEx");
    stacktraceEx.initCause(new Exception("cause"));
    stacktraceEx.getCause().setStackTrace(new StackTraceElement[0]);
    exceptionsToTest.add(stacktraceEx);

    for (Exception ex : exceptionsToTest) {
      this.testCloseClientWithExceptionRunner(ex);
    }
  }

  private void testCloseClientWithExceptionRunner(Exception exToThrow) throws Exception {
    // inject invalid client
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    SnowflakeStreamingIngestClient streamingIngestClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(streamingIngestClient.isClosed()).thenReturn(false);
    Mockito.when(streamingIngestClient.getName()).thenReturn("testclient");
    Mockito.doThrow(exToThrow).when(streamingIngestClient).close();
    StreamingClientProvider injectedProvider =
        injectStreamingClientProviderForTests(1, connectorConfig, streamingIngestClient);

    // try closing client
    injectedProvider.closeClient();

    // verify call close
    Mockito.verify(streamingIngestClient, Mockito.times(1)).close();
    Mockito.verify(streamingIngestClient, Mockito.times(1)).isClosed();
    Mockito.verify(streamingIngestClient, Mockito.times(2)).getName();
  }
}
