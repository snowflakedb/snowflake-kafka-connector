package com.snowflake.kafka.connector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SnowflakeStreamingSinkConnectorTest {

  @Test
  public void testConnectorVersionCheckDefaultsToEnabled() {
    Map<String, String> config = new HashMap<>();

    assertTrue(SnowflakeStreamingSinkConnector.isConnectorVersionCheckEnabled(config));
  }

  @Test
  public void testConnectorVersionCheckCanBeDisabled() {
    Map<String, String> config = new HashMap<>();
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_CONNECTOR_VERSION_CHECK_ENABLED,
        Boolean.FALSE.toString());

    assertFalse(SnowflakeStreamingSinkConnector.isConnectorVersionCheckEnabled(config));
  }

  @Test
  public void testConnectorVersionCheckOnlyDisablesOnExplicitFalse() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CONNECTOR_VERSION_CHECK_ENABLED, "invalid");

    assertTrue(SnowflakeStreamingSinkConnector.isConnectorVersionCheckEnabled(config));
  }
}
