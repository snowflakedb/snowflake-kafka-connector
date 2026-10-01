package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.config.ConnectorConfigDefinition;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class TaskMemoryLimitTest {

  @Test
  void configDefDefaultsToDisabled() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    Map<String, Object> parsed = configDef.parse(Map.of());
    assertEquals(-1L, parsed.get(SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES));
  }

  @Test
  void configDefAcceptsPositiveLimit() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    Map<String, Object> parsed =
        configDef.parse(Map.of(SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES, "1048576"));
    assertEquals(1_048_576L, parsed.get(SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES));
  }

  @Test
  void configDefRejectsZero() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    assertThrows(
        ConfigException.class,
        () -> configDef.parse(Map.of(SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES, "0")));
  }

  @Test
  void fromLeavesLimitDisabledByDefault() {
    SinkTaskConfig config = SinkTaskConfig.from(TestUtils.getConfig(), true);
    assertEquals(-1L, config.getMaxMemoryLimitBytes());
  }

  @Test
  void fromRejectsPositiveLimitUntilSdkExposesInflightBytes() {
    // snowpipe-streaming 1.8.1 does not have getInflightAppendedBytes(); fail closed so a
    // customer-set limit cannot silently no-op.
    assertFalse(InflightAppendedBytes.isSupported());
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES, "100");
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw, true));
    assertTrue(thrown.getMessage().contains(SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES));
    assertTrue(thrown.getMessage().contains("getInflightAppendedBytes"));
  }

  @Test
  void fromRejectsInvalidNumber() {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(SNOWFLAKE_STREAMING_MAX_MEMORY_LIMIT_BYTES, "nope");
    assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw, true));
  }
}
