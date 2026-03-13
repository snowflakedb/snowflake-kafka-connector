package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.*;
import static org.junit.jupiter.api.Assertions.*;

import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Utils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SinkTaskConfigTest {

  private static Map<String, String> minimalConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(NAME, "test_connector");
    config.put(Utils.TASK_ID, "0");
    config.put(SNOWFLAKE_URL_NAME, "https://account.snowflakecomputing.com");
    config.put(SNOWFLAKE_USER_NAME, "user");
    config.put(SNOWFLAKE_ROLE_NAME, "role");
    config.put(SNOWFLAKE_DATABASE_NAME, "db");
    config.put(SNOWFLAKE_SCHEMA_NAME, "schema");
    return config;
  }

  @Test
  public void from_minimalConfig_succeeds() {
    SinkTaskConfig config = SinkTaskConfig.from(minimalConfig());

    assertEquals("test_connector", config.getConnectorName());
    assertEquals("0", config.getTaskId());
    assertTrue(config.getTopicToTableMap().isEmpty());
    assertEquals(
        ConnectorConfigTools.BehaviorOnNullValues.DEFAULT, config.getBehaviorOnNullValues());
    assertTrue(config.isJmxEnabled());
    assertFalse(config.isTolerateErrors());
    assertNull(config.getDlqTopicName());
    assertTrue(config.isEnableSanitization());
    assertTrue(config.isEnableSchematization());
    assertTrue(config.isClientValidationEnabled());
    assertEquals(50, config.getOpenChannelIoThreads());
    assertNotNull(config.getCachingConfig());
    assertNotNull(config.getMetadataConfig());
  }

  @Test
  public void from_missingConnectorName_throws() {
    Map<String, String> config = minimalConfig();
    config.remove(NAME);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(config));
    assertTrue(e.getMessage().contains("Connector name"));
  }

  @Test
  public void from_missingTaskId_throws() {
    Map<String, String> config = minimalConfig();
    config.remove(Utils.TASK_ID);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(config));
    assertTrue(e.getMessage().contains("Task ID"));
  }

  @Test
  public void from_emptyConnectorName_throws() {
    Map<String, String> config = minimalConfig();
    config.put(NAME, "  ");

    assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(config));
  }

  @Test
  public void from_overridesDefaults() {
    Map<String, String> config = minimalConfig();
    config.put(BEHAVIOR_ON_NULL_VALUES, "ignore");
    config.put(JMX_OPT, "false");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq-topic");
    config.put(SNOWFLAKE_OPEN_CHANNEL_IO_THREADS, "10");
    config.put(SNOWFLAKE_ENABLE_SCHEMATIZATION, "false");

    SinkTaskConfig parsed = SinkTaskConfig.from(config);

    assertEquals(
        ConnectorConfigTools.BehaviorOnNullValues.IGNORE, parsed.getBehaviorOnNullValues());
    assertFalse(parsed.isJmxEnabled());
    assertTrue(parsed.isTolerateErrors());
    assertEquals("dlq-topic", parsed.getDlqTopicName());
    assertEquals(10, parsed.getOpenChannelIoThreads());
    assertFalse(parsed.isEnableSchematization());
  }

  @Test
  public void from_topic2tableMap_parsed() {
    Map<String, String> config = minimalConfig();
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, "t1:table1,t2:table2");

    SinkTaskConfig parsed = SinkTaskConfig.from(config);

    assertEquals(2, parsed.getTopicToTableMap().size());
    assertEquals("table1", parsed.getTopicToTableMap().get("t1"));
    assertEquals("table2", parsed.getTopicToTableMap().get("t2"));
  }

  @Test
  public void from_nullMap_treatedAsEmptyAndThrowsForMissingRequired() {
    // from(null) replaces null with empty map, then validation fails for missing connector name
    assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(null));
  }
}
