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

package com.snowflake.kafka.connector.internal.ingestsdk;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.ingestsdk.ClientManager;
import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ClientManagerTest {
  private Map<String, String> config;
  private String kcInstanceId;

  @Before
  // set up sunny day tests
  public void setup() {
    this.kcInstanceId = "testConnector";

    // config
    this.config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(this.config);
  }

  @Test
  public void testCreateAndGetClient() {
    // setup
    SnowflakeStreamingIngestClient goalClient =
        TestUtils.createStreamingClient(this.config, this.kcInstanceId);
    ClientManager clientManager = new ClientManager(goalClient);

    // test
    clientManager.createStreamingClient(this.config, this.kcInstanceId);
    SnowflakeStreamingIngestClient createdClient = clientManager.getStreamingIngestClient();

    // verification - very difficult (impossible?) to mock the
    // SnowflakeStreamingIngestClientFactory.builder method because it is static, so use a goal
    // client to at least test idempotency
    assert createdClient.getName().equals(goalClient.getName());
  }

  @Test
  public void testCloseClient() throws Exception {
    // setup
    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    ClientManager clientManager = new ClientManager(goalClient);

    // test
    boolean isClosed = clientManager.closeStreamingClient();

    // verify client closed
    assert isClosed;
    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
    Mockito.verify(goalClient, Mockito.times(1)).close();
  }

  @Test
  public void testCloseNullClient() {
    // setup
    ClientManager clientManager = new ClientManager(null);

    // test
    boolean isClosed = clientManager.closeStreamingClient();

    // verify client closed
    assert isClosed;
  }

  @Test
  public void testAlreadyClosedClient() throws Exception {
    // setup
    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.when(goalClient.isClosed()).thenReturn(true);
    ClientManager clientManager = new ClientManager(goalClient);

    // test
    boolean isClosed = clientManager.closeStreamingClient();

    // verify client closed
    assert isClosed;
    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
    Mockito.verify(goalClient, Mockito.times(0)).close();
  }

  @Test
  public void testCloseClientExceptionNoMessage() throws Exception {
    // setup
    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    ClientManager clientManager = new ClientManager(goalClient);
    Mockito.doThrow(new Exception()).when(goalClient).close();

    // test
    boolean isClosed = clientManager.closeStreamingClient();

    // verify
    assert !isClosed;
    Mockito.verify(goalClient, Mockito.times(1)).close();
    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
  }

  @Test
  public void testCloseClientExceptionNoCause() throws Exception {
    // setup
    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    ClientManager clientManager = new ClientManager(goalClient);
    Mockito.doThrow(new Exception("test close client failure exception")).when(goalClient).close();

    // test
    boolean isClosed = clientManager.closeStreamingClient();

    // verify
    assert !isClosed;
    Mockito.verify(goalClient, Mockito.times(1)).close();
    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
  }

  @Test
  public void testCloseClientException() throws Exception {
    // setup
    SnowflakeStreamingIngestClient goalClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    ClientManager clientManager = new ClientManager(goalClient);
    Exception closeException = new Exception("test close client failure exception");
    Exception causeException = new Exception("cause exception");
    closeException.initCause(causeException);
    Mockito.doThrow(closeException).when(goalClient).close();

    // test
    boolean isClosed = clientManager.closeStreamingClient();

    // verify
    assert !isClosed;
    Mockito.verify(goalClient, Mockito.times(1)).close();
    Mockito.verify(goalClient, Mockito.times(1)).isClosed();
  }

  @Test
  public void testGetClientFailure() {
    ClientManager clientManager = new ClientManager(null);
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_3009,
        () -> {
          clientManager.getStreamingIngestClient();
        });
  }

  @Test(expected = ConnectException.class)
  public void testMissingPropertiesForStreamingClient() {
    this.config.remove(Utils.SF_ROLE);
    ClientManager clientManager = new ClientManager(null);

    try {
      clientManager.createStreamingClient(this.config, kcInstanceId);
    } catch (ConnectException ex) {
      assert ex.getCause() instanceof SFException;
      assert ex.getCause().getMessage().contains("Missing role");
      throw ex;
    }
  }
}
