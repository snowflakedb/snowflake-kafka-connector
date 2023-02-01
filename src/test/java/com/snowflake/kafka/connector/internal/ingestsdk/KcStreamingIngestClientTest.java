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
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KcStreamingIngestClientTest {
  private String clientName;
  private Map<String, String> config;
  private Properties properties;

  private SnowflakeStreamingIngestClient actualClient;
  private SnowflakeStreamingIngestClient mockClient;

  @Before
  public void setup() {
    this.clientName = KcStreamingIngestClient.buildStreamingIngestClientName("testKcId", 0);

    this.config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    this.properties = new Properties();
    this.properties.putAll(StreamingUtils.convertConfigForStreamingClient(new HashMap<>(config)));

    // sunny day clients
    this.actualClient =
        SnowflakeStreamingIngestClientFactory.builder(this.clientName)
            .setProperties(this.properties)
            .build();
    this.mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
  }

  @After
  public void teardown() throws Exception {
    this.actualClient.close();
  }

  @Test
  public void testCreateClient() throws Exception {
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
    assert false;
  }

  @Test
  public void testOpenChannel() throws Exception {
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
  public void testCloseClientFailure() {
    assert false;
  }

  @Test
  public void testClientIsClosed() throws Exception {
    boolean isClosed = false;
    Mockito.when(this.mockClient.isClosed()).thenReturn(isClosed);
    KcStreamingIngestClient kcMockClient = new KcStreamingIngestClient(this.mockClient);
    assert kcMockClient.isClosed() == isClosed;
    Mockito.verify(this.mockClient, Mockito.times(1)).isClosed();
  }
}
