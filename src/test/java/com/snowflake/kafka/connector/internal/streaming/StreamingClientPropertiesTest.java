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

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.DEFAULT_CLIENT_NAME;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.LOGGABLE_STREAMING_CONFIG_PROPERTIES;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.STREAMING_CLIENT_PREFIX_NAME;
import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class StreamingClientPropertiesTest {

  @Test
  public void testGetValidProperties() {
    String overrideValue = "overrideValue";

    // setup config with all loggable properties and parameter override
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(Utils.NAME, "testName");
    connectorConfig.put(Utils.SF_URL, "testUrl");
    connectorConfig.put(Utils.SF_ROLE, "testRole");
    connectorConfig.put(Utils.SF_USER, "testUser");
    connectorConfig.put(Utils.SF_AUTHENTICATOR, Utils.SNOWFLAKE_JWT);
    connectorConfig.put(SNOWPIPE_STREAMING_FILE_VERSION, overrideValue);

    Properties expectedProps = StreamingUtils.convertConfigForStreamingClient(connectorConfig);
    String expectedClientName = STREAMING_CLIENT_PREFIX_NAME + connectorConfig.get(Utils.NAME);
    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(BLOB_FORMAT_VERSION, overrideValue);

    // test get properties
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // verify
    assert resultProperties.clientProperties.equals(expectedProps);
    assert resultProperties.clientName.equals(expectedClientName);
    assert resultProperties.parameterOverrides.equals(expectedParameterOverrides);

    // verify only loggable props are returned
    String loggableProps = resultProperties.getLoggableClientProperties();
    for (Object key : expectedProps.keySet()) {
      Object value = expectedProps.get(key);
      if (LOGGABLE_STREAMING_CONFIG_PROPERTIES.stream()
          .anyMatch(key.toString()::equalsIgnoreCase)) {
        assert loggableProps.contains(
            Utils.formatString("{}={}", key.toString(), value.toString()));
      } else {
        assert !loggableProps.contains(key.toString()) && !loggableProps.contains(value.toString());
      }
    }
  }

  @Test
  public void testGetInvalidProperties() {
    StreamingClientProperties nullProperties = new StreamingClientProperties(null);
    StreamingClientProperties emptyProperties = new StreamingClientProperties(new HashMap<>());

    assert nullProperties.equals(emptyProperties);
    assert nullProperties.clientName.equals(STREAMING_CLIENT_PREFIX_NAME + DEFAULT_CLIENT_NAME);
    assert nullProperties.getLoggableClientProperties().equals("");
  }

  @Test
  public void testStreamingClientPropertiesEquality() {
    Map<String, String> config1 = TestUtils.getConfForStreaming();
    config1.put(Utils.NAME, "catConnector");
    config1.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "100");

    Map<String, String> config2 = TestUtils.getConfForStreaming();
    config1.put(Utils.NAME, "dogConnector");
    config1.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "1000000");

    // get properties
    StreamingClientProperties prop1 = new StreamingClientProperties(config1);
    StreamingClientProperties prop2 = new StreamingClientProperties(config2);

    assert prop1.equals(prop2);
    assert prop1.hashCode() == prop2.hashCode();
  }
}
