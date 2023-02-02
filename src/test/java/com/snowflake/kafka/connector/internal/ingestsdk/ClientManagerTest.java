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
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ClientManagerTest {
  private Map<String, String> config;
  private Properties properties;
  private String kcInstanceId;

  private KcStreamingIngestClient mockClient;

  @Before
  public void setUp() {
    this.config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    this.properties = new Properties();
    this.properties.putAll(StreamingUtils.convertConfigForStreamingClient(new HashMap<>(config)));
    this.kcInstanceId = "testkcid";

    this.mockClient = Mockito.mock(KcStreamingIngestClient.class);
  }

  @Test
  public void testCreateAndGetAllStreamingClients() {
    // test to create the following mapping
    // [0, 1] -> clientA, [2, 3] -> clientB, [4] -> clientC
    // test
    ClientManager manager = new ClientManager();
    manager.createAllStreamingClients(config, kcInstanceId, 5, 2);

    // verify
    KcStreamingIngestClient task0Client = manager.getValidClient(0);
    KcStreamingIngestClient task1Client = manager.getValidClient(1);
    assert task0Client.equals(task1Client);

    KcStreamingIngestClient task2Client = manager.getValidClient(2);
    KcStreamingIngestClient task3Client = manager.getValidClient(3);
    assert task2Client.equals(task3Client);
    assert !task2Client.equals(task0Client);

    KcStreamingIngestClient task4Client = manager.getValidClient(4);
    assert !task4Client.equals(task0Client);
    assert !task4Client.equals(task2Client);

    // close clients
    task0Client.close();
    task1Client.close();
    task2Client.close();
    task3Client.close();
    task4Client.close();
  }

  @Test
  public void testCloseAllStreamingClients() {
    // test to close the following mapping
    // [0, 1] -> clientA, [2, 3] -> clientB, [4] -> clientC
    KcStreamingIngestClient task01Client = Mockito.mock(KcStreamingIngestClient.class);
    KcStreamingIngestClient task23Client = Mockito.mock(KcStreamingIngestClient.class);
    KcStreamingIngestClient task4Client = Mockito.mock(KcStreamingIngestClient.class);

    Mockito.when(task01Client.close()).thenReturn(true);
    Mockito.when(task23Client.close()).thenReturn(true);
    Mockito.when(task4Client.close()).thenReturn(true);

    Map<Integer, KcStreamingIngestClient> taskToClientMap = new HashMap<>();
    taskToClientMap.put(0, task01Client);
    taskToClientMap.put(1, task01Client);
    taskToClientMap.put(2, task23Client);
    taskToClientMap.put(3, task23Client);
    taskToClientMap.put(4, task4Client);

    ClientManager manager = new ClientManager(taskToClientMap);

    // test
    assert manager.closeAllStreamingClients();

    // verify
    Mockito.verify(task01Client, Mockito.times(2)).close();
    Mockito.verify(task23Client, Mockito.times(2)).close();
    Mockito.verify(task4Client, Mockito.times(1)).close();
  }

  @Test
  public void testGetClientFailure() {
    assert false;
  }
}
