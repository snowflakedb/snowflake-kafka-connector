package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TaskInflightCapsConfigTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES
      })
  void configDefDefaultsToDisabled(String key) {
    Map<String, Object> parsed = ConnectorConfigDefinition.getConfig().parse(Map.of());
    assertEquals(0L, parsed.get(key));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES
      })
  void configDefAcceptsZeroAndPositive(String key) {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    assertEquals(0L, configDef.parse(Map.of(key, "0")).get(key));
    assertEquals(4096L, configDef.parse(Map.of(key, "4096")).get(key));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES
      })
  void configDefRejectsNegative(String key) {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    assertThrows(ConfigException.class, () -> configDef.parse(Map.of(key, "-1")));
  }

  @Test
  void fromLeavesBothCapsDisabledByDefault() {
    SinkTaskConfig config = SinkTaskConfig.from(TestUtils.getConfig(), true);
    assertEquals(0L, config.getMaxTaskInflightRows());
    assertEquals(0L, config.getMaxTaskInflightBytes());
  }

  @Test
  void fromParsesRowsAndConvertsMegabytesToBytes() {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS, "1000");
    raw.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES, "64");

    SinkTaskConfig config = SinkTaskConfig.from(raw, true);

    assertEquals(1000L, config.getMaxTaskInflightRows());
    assertEquals(64L * 1024 * 1024, config.getMaxTaskInflightBytes());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES
      })
  void fromRejectsNegative(String key) {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(key, "-1");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw, true));
    assertTrue(thrown.getMessage().contains(key), thrown.getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
        SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES
      })
  void fromRejectsNonNumeric(String key) {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(key, "nope");

    assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw, true));
  }

  @Test
  void fromRejectsMegabytesThatOverflowBytes() {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES, String.valueOf(Long.MAX_VALUE));

    assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw, true));
  }
}
