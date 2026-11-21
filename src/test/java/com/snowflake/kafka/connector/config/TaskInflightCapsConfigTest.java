package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class TaskInflightCapsConfigTest {

  @Test
  void limitsAreOffByDefaultAndNegativeAndOverflowingValuesAreRejected() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    assertEquals(0L, configDef.parse(Map.of()).get(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS));
    assertEquals(
        0L, configDef.parse(Map.of()).get(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES));
    assertThrows(
        ConfigException.class,
        () -> configDef.parse(Map.of(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS, "-1")));
    assertThrows(
        ConfigException.class,
        () -> configDef.parse(Map.of(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES, "-1")));
    assertThrows(
        ConfigException.class,
        () ->
            configDef.parse(
                Map.of(
                    SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES,
                    String.valueOf((Long.MAX_VALUE >> 20) + 1))));
  }

  @Test
  void rowsPassThroughAndMegabytesConvertToBytes() {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS, "1000");
    raw.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES, "64");

    SinkTaskConfig config = SinkTaskConfig.from(raw, true);

    assertEquals(1000L, config.getMaxTaskInflightRows());
    assertEquals(64L * 1024 * 1024, config.getMaxTaskInflightBytes());
  }
}
