package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DefaultStreamingConfigValidatorTest {

  private final DefaultStreamingConfigValidator validator = new DefaultStreamingConfigValidator();

  private Map<String, String> validRawConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.role.name", "testrole");
    return config;
  }

  private ImmutableMap<String, String> validate(Map<String, String> rawConfig) {
    SinkTaskConfig parsedConfig = SinkTaskConfig.from(rawConfig);
    return validator.validate(parsedConfig, rawConfig);
  }

  @Test
  void testStringConverterAllowed_WhenSchematizationDisabled() {
    Map<String, String> config = validRawConfig();
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("snowflake.enable.schematization", "false");

    ImmutableMap<String, String> result = validate(config);

    assertTrue(
        result.isEmpty(), "StringConverter should be allowed when schematization is disabled");
  }

  @Test
  void testByteArrayConverterAllowed_WhenSchematizationDisabled() {
    Map<String, String> config = validRawConfig();
    config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    config.put("snowflake.enable.schematization", "false");

    ImmutableMap<String, String> result = validate(config);

    assertTrue(
        result.isEmpty(), "ByteArrayConverter should be allowed when schematization is disabled");
  }

  @Test
  void testStringConverterBlocked_WhenSchematizationEnabled() {
    Map<String, String> config = validRawConfig();
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("snowflake.enable.schematization", "true");

    ImmutableMap<String, String> result = validate(config);

    assertFalse(
        result.isEmpty(), "StringConverter should be blocked when schematization is enabled");
  }

  @Test
  void testByteArrayConverterBlocked_WhenSchematizationEnabled() {
    Map<String, String> config = validRawConfig();
    config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    config.put("snowflake.enable.schematization", "true");

    ImmutableMap<String, String> result = validate(config);

    assertFalse(
        result.isEmpty(), "ByteArrayConverter should be blocked when schematization is enabled");
  }

  @Test
  void testStringConverterBlocked_WhenSchematizationDefault() {
    Map<String, String> config = validRawConfig();
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

    ImmutableMap<String, String> result = validate(config);

    assertFalse(
        result.isEmpty(), "StringConverter should be blocked when schematization defaults to true");
  }

  @Test
  void testJsonConverterAllowed_WhenSchematizationEnabled() {
    Map<String, String> config = validRawConfig();
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("snowflake.enable.schematization", "true");

    ImmutableMap<String, String> result = validate(config);

    assertTrue(result.isEmpty(), "JsonConverter should be allowed regardless of schematization");
  }
}
