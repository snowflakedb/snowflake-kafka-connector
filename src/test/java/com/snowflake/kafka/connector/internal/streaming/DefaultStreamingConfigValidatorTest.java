package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DefaultStreamingConfigValidatorTest {

  private final DefaultStreamingConfigValidator validator = new DefaultStreamingConfigValidator();

  private Map<String, String> validConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.role.name", "testrole");
    return config;
  }

  @Test
  void testStringConverterAllowed_WhenSchematizationDisabled() {
    Map<String, String> config = validConfig();
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("snowflake.enable.schematization", "false");

    ImmutableMap<String, String> result = validator.validate(config);

    assertTrue(
        result.isEmpty(), "StringConverter should be allowed when schematization is disabled");
  }

  @Test
  void testByteArrayConverterAllowed_WhenSchematizationDisabled() {
    Map<String, String> config = validConfig();
    config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    config.put("snowflake.enable.schematization", "false");

    ImmutableMap<String, String> result = validator.validate(config);

    assertTrue(
        result.isEmpty(), "ByteArrayConverter should be allowed when schematization is disabled");
  }

  @Test
  void testStringConverterBlocked_WhenSchematizationEnabled() {
    Map<String, String> config = validConfig();
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("snowflake.enable.schematization", "true");

    ImmutableMap<String, String> result = validator.validate(config);

    assertFalse(
        result.isEmpty(), "StringConverter should be blocked when schematization is enabled");
  }

  @Test
  void testByteArrayConverterBlocked_WhenSchematizationEnabled() {
    Map<String, String> config = validConfig();
    config.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    config.put("snowflake.enable.schematization", "true");

    ImmutableMap<String, String> result = validator.validate(config);

    assertFalse(
        result.isEmpty(), "ByteArrayConverter should be blocked when schematization is enabled");
  }

  @Test
  void testStringConverterBlocked_WhenSchematizationDefault() {
    Map<String, String> config = validConfig();
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

    ImmutableMap<String, String> result = validator.validate(config);

    assertFalse(
        result.isEmpty(), "StringConverter should be blocked when schematization defaults to true");
  }

  @Test
  void testJsonConverterAllowed_WhenSchematizationEnabled() {
    Map<String, String> config = validConfig();
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("snowflake.enable.schematization", "true");

    ImmutableMap<String, String> result = validator.validate(config);

    assertTrue(result.isEmpty(), "JsonConverter should be allowed regardless of schematization");
  }

  /** SNOW-4392681: under SPCS the service owner role applies, so no role is required. */
  @Test
  void testMissingRoleAllowed_WhenAuthenticatorIsSpcs() {
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.authenticator", "spcs");

    ImmutableMap<String, String> result = validator.validate(config);

    assertFalse(
        result.containsKey("snowflake.role.name"), "role should be optional under spcs: " + result);
  }

  @Test
  void testMissingRoleRejected_WhenAuthenticatorIsNotSpcs() {
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.authenticator", "snowflake_jwt");

    ImmutableMap<String, String> result = validator.validate(config);

    assertTrue(
        result.containsKey("snowflake.role.name"), "role should be required for snowflake_jwt");
  }

  @Test
  void testMissingRoleRejected_WhenAuthenticatorIsAbsent() {
    ImmutableMap<String, String> result = validator.validate(new HashMap<>());

    assertTrue(result.containsKey("snowflake.role.name"), "role should be required by default");
  }
}
