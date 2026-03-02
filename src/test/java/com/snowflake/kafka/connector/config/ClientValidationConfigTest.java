package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

public class ClientValidationConfigTest {

  @Test
  public void testClientValidationConfigExists() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    // Verify the config key exists in the definition
    assertNotNull(
        configDef.configKeys().get(SNOWFLAKE_CLIENT_VALIDATION_ENABLED),
        "snowflake.client.validation.enabled should be defined in config");
  }

  @Test
  public void testClientValidationDefaultValue() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    // Verify default value is true
    Object defaultValue =
        configDef.configKeys().get(SNOWFLAKE_CLIENT_VALIDATION_ENABLED).defaultValue;

    assertEquals(
        SNOWFLAKE_CLIENT_VALIDATION_ENABLED_DEFAULT, defaultValue, "Default value should be true");
    assertTrue((Boolean) defaultValue, "Default value should be true");
  }

  @Test
  public void testClientValidationConfigCanBeSetToFalse() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    Map<String, String> props = new HashMap<>();
    props.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "false");

    // Parse the config with validation disabled
    Map<String, Object> parsed = configDef.parse(props);

    // Verify it can be set to false
    assertEquals(
        false,
        parsed.get(SNOWFLAKE_CLIENT_VALIDATION_ENABLED),
        "Should be able to set validation to false");
  }

  @Test
  public void testClientValidationConfigCanBeSetToTrue() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    Map<String, String> props = new HashMap<>();
    props.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");

    // Parse the config with validation enabled
    Map<String, Object> parsed = configDef.parse(props);

    // Verify it can be explicitly set to true
    assertEquals(
        true,
        parsed.get(SNOWFLAKE_CLIENT_VALIDATION_ENABLED),
        "Should be able to set validation to true");
  }

  @Test
  public void testClientValidationConfigDefaultsToTrue() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    Map<String, String> props = new HashMap<>();
    // Don't set the parameter - should use default

    // Parse the config without specifying the validation parameter
    Map<String, Object> parsed = configDef.parse(props);

    // Verify default is true
    assertEquals(
        true,
        parsed.get(SNOWFLAKE_CLIENT_VALIDATION_ENABLED),
        "Should default to true when not specified");
  }
}
