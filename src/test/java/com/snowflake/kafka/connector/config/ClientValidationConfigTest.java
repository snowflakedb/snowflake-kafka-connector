package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

public class ClientValidationConfigTest {

  @Test
  public void testValidationConfigExists() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    assertNotNull(
        configDef.configKeys().get(SNOWFLAKE_VALIDATION),
        "snowflake.validation should be defined in config");
  }

  @Test
  public void testValidationDefaultValue() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    Object defaultValue = configDef.configKeys().get(SNOWFLAKE_VALIDATION).defaultValue;

    assertEquals(SNOWFLAKE_VALIDATION_DEFAULT, defaultValue, "Default value should be server_side");
  }

  @Test
  public void testValidationCanBeSetToServerSide() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    Map<String, String> props = new HashMap<>();
    props.put(SNOWFLAKE_VALIDATION, "server_side");

    Map<String, Object> parsed = configDef.parse(props);

    assertEquals(
        "server_side",
        parsed.get(SNOWFLAKE_VALIDATION),
        "Should be able to set validation to server_side");
  }

  @Test
  public void testValidationCanBeSetToClientSide() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    Map<String, String> props = new HashMap<>();
    props.put(SNOWFLAKE_VALIDATION, "client_side");

    Map<String, Object> parsed = configDef.parse(props);

    assertEquals(
        "client_side",
        parsed.get(SNOWFLAKE_VALIDATION),
        "Should be able to set validation to client_side");
  }

  @Test
  public void testValidationDefaultsToServerSide() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();

    Map<String, String> props = new HashMap<>();

    Map<String, Object> parsed = configDef.parse(props);

    assertEquals(
        "server_side",
        parsed.get(SNOWFLAKE_VALIDATION),
        "Should default to server_side when not specified");
  }
}
