package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SnowflakeSinkTaskInflightCapsTest {

  // The pinned snowpipe-streaming 1.9.0 does not publish the per-channel inflight JMX beans, so
  // start() must refuse an enabled cap before it opens any connection.
  @ParameterizedTest
  @ValueSource(
      strings = {
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES
      })
  void startFailsWhenCapEnabledAndBundledSdkLacksInflightBeans(String key) {
    Map<String, String> config = new HashMap<>(TestUtils.getConfig());
    config.put(Utils.TASK_ID, "0");
    config.put(key, "10");

    ConnectException thrown =
        assertThrows(ConnectException.class, () -> new SnowflakeSinkTask().start(config));
    assertTrue(
        thrown.getMessage().contains(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS),
        thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES),
        thrown.getMessage());
  }
}
