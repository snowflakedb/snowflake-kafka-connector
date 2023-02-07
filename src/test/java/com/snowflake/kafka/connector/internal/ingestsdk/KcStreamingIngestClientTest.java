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
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KcStreamingIngestClientTest {
  private String clientName;
  private Map<String, String> config;
  private Properties properties;

  private SnowflakeStreamingIngestClient mockClient;

  @Before
  public void setup() {
    this.clientName = KcStreamingIngestClient.buildStreamingIngestClientName("testKcId", 0);

    this.config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    this.properties = new Properties();
    this.properties.putAll(StreamingUtils.convertConfigForStreamingClient(new HashMap<>(config)));

    this.mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
  }

  @Test
  public void testCreateClient() {
    // setup
    Mockito.when(this.mockClient.getName()).thenReturn(this.clientName);
    KcStreamingIngestClient kcMockClient = new KcStreamingIngestClient(this.mockClient);

    // test
    KcStreamingIngestClient kcActualClient =
        new KcStreamingIngestClient(this.properties, this.clientName);

    // verify
    assert kcActualClient.getName().equals(kcMockClient.getName());
    Mockito.verify(this.mockClient, Mockito.times(1)).getName();
  }

  @Test
  public void testCreateClientFailure() {
    TestUtils.assertExceptionType(
        ConnectException.class, () -> new KcStreamingIngestClient(null, null));
    TestUtils.assertExceptionType(
        ConnectException.class, () -> new KcStreamingIngestClient(null, this.clientName));
    TestUtils.assertExceptionType(
        ConnectException.class, () -> new KcStreamingIngestClient(this.properties, null));
  }

  @Test
  public void testOpenChannel() {
    String channelName = "testchannel";
    String tableName = "testtable";
    this.config.put(Utils.SF_DATABASE, "testdb");
    this.config.put(Utils.SF_SCHEMA, "testschema");
    OpenChannelRequest request =
        OpenChannelRequest.builder(channelName)
            .setDBName(this.config.get(Utils.SF_DATABASE))
            .setSchemaName(this.config.get(Utils.SF_SCHEMA))
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // setup mocks
    SnowflakeStreamingIngestChannel goalChannel =
        Mockito.mock(SnowflakeStreamingIngestChannel.class);
    Mockito.when(this.mockClient.openChannel(ArgumentMatchers.refEq(request)))
        .thenReturn(goalChannel);

    // test
    KcStreamingIngestClient kcMockClient = new KcStreamingIngestClient(this.mockClient);
    SnowflakeStreamingIngestChannel res =
        kcMockClient.openChannel(channelName, this.config, tableName);

    // verify
    assert res.equals(goalChannel);
    Mockito.verify(this.mockClient, Mockito.times(1)).openChannel(ArgumentMatchers.refEq(request));
  }

  @Test
  public void testCloseClient() throws Exception {
    Mockito.when(this.mockClient.isClosed()).thenReturn(false);
    KcStreamingIngestClient kcMockClient = new KcStreamingIngestClient(this.mockClient);
    assert kcMockClient.close();
    Mockito.verify(this.mockClient, Mockito.times(1)).close();
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
  }

  @Test
  public void testCloseAlreadyClosedClient() throws Exception {
    Mockito.when(this.mockClient.isClosed()).thenReturn(true);
    KcStreamingIngestClient kcMockClient = new KcStreamingIngestClient(this.mockClient);
    assert kcMockClient.close();
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
  }

  @Test
  public void testCloseClientFailure() throws Exception {
    Exception exceptionToThrow = new Exception();
    this.testCloseClientFailureRunner(exceptionToThrow);
    exceptionToThrow = new Exception("did you pet a cat today though");
    this.testCloseClientFailureRunner(exceptionToThrow);
    exceptionToThrow.initCause(new Exception("because you should"));
    this.testCloseClientFailureRunner(exceptionToThrow);
  }

  private void testCloseClientFailureRunner(Exception exceptionToThrow) throws Exception {
    this.mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    Mockito.doThrow(exceptionToThrow).when(this.mockClient).close();
    Mockito.when(this.mockClient.isClosed()).thenReturn(false);

    // test
    KcStreamingIngestClient kcMockClient = new KcStreamingIngestClient(this.mockClient);
    assert !kcMockClient.close();

    // verify
    Mockito.verify(this.mockClient, Mockito.times(1)).close();
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
  }

  @Test
  public void testClientIsClosed() {
    boolean isClosed = false;
    Mockito.when(this.mockClient.isClosed()).thenReturn(isClosed);
    KcStreamingIngestClient kcMockClient = new KcStreamingIngestClient(this.mockClient);
    assert kcMockClient.isClosed() == isClosed;
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
  }
}
