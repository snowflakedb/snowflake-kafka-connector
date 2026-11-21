package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

class SnowflakeSinkTaskInflightCapsTest {

  @Test
  void startFailsWhenCapEnabledAndBundledSdkLacksInflightBeans() {
    Map<String, String> config = new HashMap<>(TestUtils.getConfig());
    config.put(Utils.TASK_ID, "0");
    config.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS, "10");

    ConnectException thrown =
        assertThrows(ConnectException.class, () -> new SnowflakeSinkTask().start(config));
    assertTrue(
        thrown.getMessage().contains(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS),
        thrown.getMessage());
  }
}
