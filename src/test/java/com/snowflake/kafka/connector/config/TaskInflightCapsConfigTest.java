package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TaskInflightCapsConfigTest {

  private static final long BYTES_PER_MEGABYTE = 1024L * 1024L;

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

  @ParameterizedTest
  @MethodSource("keysWithBlankValues")
  void fromTreatsBlankAsDisabled(String key, String blank) {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(key, blank);

    SinkTaskConfig config = SinkTaskConfig.from(raw, true);

    assertEquals(0L, config.getMaxTaskInflightRows());
    assertEquals(0L, config.getMaxTaskInflightBytes());
  }

  static Stream<Arguments> keysWithBlankValues() {
    return Stream.of(
            SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
            SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES)
        .flatMap(key -> Stream.of(Arguments.of(key, ""), Arguments.of(key, "  ")));
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
  @MethodSource("keysWithNonLongValues")
  void fromRejectsNonLong(String key, String value) {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(key, value);

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw, true));
    assertEquals(IllegalArgumentException.class, thrown.getClass());
    assertInstanceOf(NumberFormatException.class, thrown.getCause());
    assertTrue(thrown.getMessage().contains(key), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(value), thrown.getMessage());
  }

  static Stream<Arguments> keysWithNonLongValues() {
    return Stream.of(
            SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
            SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES)
        .flatMap(
            key ->
                Stream.of("nope", "1.5", "1e6", "64MB", "99999999999999999999")
                    .map(value -> Arguments.of(key, value)));
  }

  @Test
  void fromAcceptsLargestMegabytesThatFitInBytes() {
    long largest = Long.MAX_VALUE / BYTES_PER_MEGABYTE;
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES, String.valueOf(largest));

    assertEquals(
        largest * BYTES_PER_MEGABYTE, SinkTaskConfig.from(raw, true).getMaxTaskInflightBytes());
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MAX_VALUE / (1024L * 1024L) + 1, (1L << 44) + 1, Long.MAX_VALUE})
  void fromRejectsMegabytesThatOverflowBytes(long megabytes) {
    Map<String, String> raw = new HashMap<>(TestUtils.getConfig());
    raw.put(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES, String.valueOf(megabytes));

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw, true));
    assertTrue(
        thrown.getMessage().contains(SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES),
        thrown.getMessage());
    assertTrue(thrown.getMessage().contains("too large"), thrown.getMessage());
    assertInstanceOf(ArithmeticException.class, thrown.getCause());
  }
}
