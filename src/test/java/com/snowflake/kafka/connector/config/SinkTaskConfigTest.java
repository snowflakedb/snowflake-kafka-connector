package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.*;
import static org.junit.jupiter.api.Assertions.*;

import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationMode;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.types.Password;
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
    assertFalse(config.isEnableSanitization());
    assertTrue(config.isEnableSchematization());
    assertEquals(SnowflakeValidation.SERVER_SIDE, config.getValidation());
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
    assertEquals("TABLE1", parsed.getTopicToTableMap().get("t1"));
    assertEquals("TABLE2", parsed.getTopicToTableMap().get("t2"));
  }

  @Test
  public void from_nullMap_treatedAsEmptyAndThrowsForMissingRequired() {
    // from(null) replaces null with empty map, then validation fails for missing connector name
    assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(null));
  }

  @Test
  public void from_defaultMigrationMode_isSkip() {
    SinkTaskConfig config = SinkTaskConfig.from(minimalConfig());
    assertEquals(Ssv1MigrationMode.SKIP, config.getSsv1MigrationMode());
  }

  @Test
  public void from_migrationMode_bestEffort() {
    Map<String, String> config = minimalConfig();
    config.put(SNOWFLAKE_SSV1_OFFSET_MIGRATION, "best_effort");

    SinkTaskConfig parsed = SinkTaskConfig.from(config);
    assertEquals(Ssv1MigrationMode.BEST_EFFORT, parsed.getSsv1MigrationMode());
  }

  @Test
  public void from_migrationMode_strict() {
    Map<String, String> config = minimalConfig();
    config.put(SNOWFLAKE_SSV1_OFFSET_MIGRATION, "strict");

    SinkTaskConfig parsed = SinkTaskConfig.from(config);
    assertEquals(Ssv1MigrationMode.STRICT, parsed.getSsv1MigrationMode());
  }

  @Test
  public void from_migrationMode_caseInsensitive() {
    Map<String, String> config = minimalConfig();
    config.put(SNOWFLAKE_SSV1_OFFSET_MIGRATION, "BEST_EFFORT");

    SinkTaskConfig parsed = SinkTaskConfig.from(config);
    assertEquals(Ssv1MigrationMode.BEST_EFFORT, parsed.getSsv1MigrationMode());
  }

  @Test
  public void from_migrationMode_invalidValue_throws() {
    Map<String, String> config = minimalConfig();
    config.put(SNOWFLAKE_SSV1_OFFSET_MIGRATION, "invalid_value");

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(config));
    assertTrue(ex.getMessage().contains(SNOWFLAKE_SSV1_OFFSET_MIGRATION));
    assertTrue(ex.getMessage().contains("invalid_value"));
  }

  @Test
  public void from_defaultIncludeConnectorName_isFalse() {
    SinkTaskConfig config = SinkTaskConfig.from(minimalConfig());
    assertFalse(config.isSsv1MigrationIncludeConnectorName());
  }

  @Test
  public void from_includeConnectorNameTrue_isParsed() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME, "true");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertTrue(config.isSsv1MigrationIncludeConnectorName());
  }

  @Test
  public void from_oauthFields_areParsed() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTHENTICATOR, AuthenticatorType.OAUTH.toConfigValue());
    raw.put(SNOWFLAKE_OAUTH_CLIENT_ID, "my_client_id");
    raw.put(SNOWFLAKE_OAUTH_CLIENT_SECRET, "my_client_secret");
    raw.put(SNOWFLAKE_OAUTH_REFRESH_TOKEN, "my_refresh_token");
    raw.put(SNOWFLAKE_OAUTH_TOKEN_ENDPOINT, "https://oauth.example.com/token");

    SinkTaskConfig config = SinkTaskConfig.from(raw);

    assertEquals(AuthenticatorType.OAUTH, config.getAuthenticator());
    assertEquals("my_client_id", config.getOauthClientId().orElse(null));
    assertEquals(
        "my_client_secret", config.getOauthClientSecret().map(Password::value).orElse(null));
    assertEquals(
        "my_refresh_token", config.getOauthRefreshToken().map(Password::value).orElse(null));
    assertEquals("https://oauth.example.com/token", config.getOauthTokenEndpoint().orElse(null));
    assertFalse(config.getOauthIncludeScope());
    assertTrue(config.getOauthScope().isEmpty());
  }

  @Test
  public void from_oauthScopeFields_areParsed() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTHENTICATOR, AuthenticatorType.OAUTH.toConfigValue());
    raw.put(SNOWFLAKE_OAUTH_CLIENT_ID, "my_client_id");
    raw.put(SNOWFLAKE_OAUTH_CLIENT_SECRET, "my_client_secret");
    raw.put(SNOWFLAKE_OAUTH_INCLUDE_SCOPE, "true");
    raw.put(SNOWFLAKE_OAUTH_SCOPE, "session:role:MY_ROLE");

    SinkTaskConfig config = SinkTaskConfig.from(raw);

    assertTrue(config.getOauthIncludeScope());
    assertEquals("session:role:MY_ROLE", config.getOauthScope().orElse(null));
  }

  @Test
  public void from_privateKeyFields_wrappedAsPassword() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_PRIVATE_KEY, "my_private_key");
    raw.put(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, "my_passphrase");

    SinkTaskConfig config = SinkTaskConfig.from(raw);

    assertEquals(
        "my_private_key", config.getSnowflakePrivateKey().map(Password::value).orElse(null));
    assertEquals(
        "my_passphrase",
        config.getSnowflakePrivateKeyPassphrase().map(Password::value).orElse(null));
  }

  @Test
  public void from_missingPrivateKey_returnsEmpty() {
    SinkTaskConfig config = SinkTaskConfig.from(minimalConfig());

    assertTrue(config.getSnowflakePrivateKey().isEmpty());
    assertTrue(config.getSnowflakePrivateKeyPassphrase().isEmpty());
  }

  @Test
  public void from_defaultAuthenticator_isSnowflakeJwt() {
    SinkTaskConfig config = SinkTaskConfig.from(minimalConfig());
    assertEquals(AuthenticatorType.SNOWFLAKE_JWT, config.getAuthenticator());
  }

  @Test
  public void from_oauthWithoutOptionalFields_succeeds() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTHENTICATOR, AuthenticatorType.OAUTH.toConfigValue());
    raw.put(SNOWFLAKE_OAUTH_CLIENT_ID, "client_id");
    raw.put(SNOWFLAKE_OAUTH_CLIENT_SECRET, "client_secret");

    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertEquals(AuthenticatorType.OAUTH, config.getAuthenticator());
    assertTrue(config.getOauthRefreshToken().isEmpty());
    assertTrue(config.getOauthTokenEndpoint().isEmpty());
    assertFalse(config.getOauthIncludeScope());
    assertTrue(config.getOauthScope().isEmpty());
  }

  @Test
  public void from_skipTaskSpecificConfig_succeedsWithoutTaskId() {
    Map<String, String> raw = minimalConfig();
    raw.remove(Utils.TASK_ID);
    SinkTaskConfig config = SinkTaskConfig.from(raw, true);
    assertEquals("", config.getTaskId());
    assertEquals("test_connector", config.getConnectorName());
  }

  @Test
  public void from_skipTaskSpecificConfig_succeedsWithoutConnectorName() {
    Map<String, String> raw = minimalConfig();
    raw.remove(NAME);
    raw.remove(Utils.TASK_ID);
    SinkTaskConfig config = SinkTaskConfig.from(raw, true);
    assertEquals("", config.getConnectorName());
    assertEquals("", config.getTaskId());
  }

  @Test
  public void from_skipTaskSpecificConfig_false_throwsWithoutTaskId() {
    Map<String, String> raw = minimalConfig();
    raw.remove(Utils.TASK_ID);
    assertThrows(IllegalArgumentException.class, () -> SinkTaskConfig.from(raw));
  }
}
