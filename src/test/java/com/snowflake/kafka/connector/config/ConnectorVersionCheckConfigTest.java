package com.snowflake.kafka.connector.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

public class ConnectorVersionCheckConfigTest {

  @Test
  public void testConnectorVersionCheckConfigExists() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    String key = KafkaConnectorConfigParams.SNOWFLAKE_CONNECTOR_VERSION_CHECK_ENABLED;

    assertNotNull(
        configDef.configKeys().get(key),
        "snowflake.connector.version.check.enabled should be defined in config");
  }

  @Test
  public void testConnectorVersionCheckDefaultsToEnabled() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    String key = KafkaConnectorConfigParams.SNOWFLAKE_CONNECTOR_VERSION_CHECK_ENABLED;

    Object defaultValue = configDef.configKeys().get(key).defaultValue;

    assertEquals(
        KafkaConnectorConfigParams.SNOWFLAKE_CONNECTOR_VERSION_CHECK_ENABLED_DEFAULT,
        defaultValue,
        "Connector version check should be enabled by default");
  }

  @Test
  public void testConnectorVersionCheckCanBeDisabled() {
    ConfigDef configDef = ConnectorConfigDefinition.getConfig();
    String key = KafkaConnectorConfigParams.SNOWFLAKE_CONNECTOR_VERSION_CHECK_ENABLED;

    Map<String, String> props = new HashMap<>();
    props.put(key, "false");

    Map<String, Object> parsed = configDef.parse(props);

    assertEquals(
        Boolean.FALSE, parsed.get(key), "Connector version check should be configurable");
  }
}
