package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    assertNull(config.getTopicToTableResolver().resolve("anything"));
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

    assertEquals(2, parsed.getTopicToTableResolver().tableNames().size());
    assertEquals("TABLE1", parsed.getTopicToTableResolver().resolve("t1"));
    assertEquals("TABLE2", parsed.getTopicToTableResolver().resolve("t2"));
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

  @Test
  void from_defaultsTableTypeToSnowflake() {
    SinkTaskConfig config = SinkTaskConfig.from(minimalConfig());
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.SNOWFLAKE);
    assertThat(config.getIcebergCreateTableOptions()).isEmpty();
  }

  @Test
  void from_parsesIcebergTableTypeAndCreateOptions() {
    Map<String, String> raw = minimalConfig();
    raw.put("snowflake.autocreate.table.type", "iceberg");
    // client_side is always rejected for iceberg; server_side is fine.
    raw.put("snowflake.validation", "server_side");
    raw.put("snowflake.iceberg.create.table.options", "EXTERNAL_VOLUME='my_vol' ICEBERG_VERSION=3");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.ICEBERG);
    assertThat(config.getIcebergCreateTableOptions())
        .contains("EXTERNAL_VOLUME='my_vol' ICEBERG_VERSION=3");
  }

  @Test
  void from_icebergWithClientSideValidation_throws() {
    // Rejected regardless of whether schematization is enabled or not.
    Map<String, String> raw = minimalConfig();
    raw.put("snowflake.autocreate.table.type", "iceberg");
    raw.put("snowflake.enable.schematization", "true");
    raw.put("snowflake.validation", "client_side");
    assertThatThrownBy(() -> SinkTaskConfig.from(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("client_side")
        .hasMessageContaining("server_side");
  }

  @Test
  void from_icebergWithClientSideValidationNoSchematization_throws() {
    // client_side is also rejected when schematization is disabled.
    Map<String, String> raw = minimalConfig();
    raw.put("snowflake.autocreate.table.type", "iceberg");
    raw.put("snowflake.enable.schematization", "false");
    raw.put("snowflake.validation", "client_side");
    assertThatThrownBy(() -> SinkTaskConfig.from(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("client_side")
        .hasMessageContaining("server_side");
  }

  @Test
  void from_icebergWithServerSideValidationAndSchematization_ok() {
    Map<String, String> raw = minimalConfig();
    raw.put("snowflake.autocreate.table.type", "iceberg");
    raw.put("snowflake.enable.schematization", "true");
    raw.put("snowflake.validation", "server_side");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.ICEBERG);
    assertThat(config.getValidation()).isEqualTo(SnowflakeValidation.SERVER_SIDE);
  }

  @Test
  void from_createOptionsWithoutIcebergTableType_throws() {
    Map<String, String> raw = minimalConfig();
    raw.put("snowflake.autocreate.table.type", "snowflake");
    raw.put("snowflake.iceberg.create.table.options", "EXTERNAL_VOLUME='my_vol'");
    assertThatThrownBy(() -> SinkTaskConfig.from(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snowflake.iceberg.create.table.options");
  }

  @Test
  void from_blankCreateOptionsWithoutIcebergTableType_ok() {
    Map<String, String> raw = minimalConfig();
    raw.put("snowflake.autocreate.table.type", "snowflake");
    raw.put("snowflake.iceberg.create.table.options", "   ");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getIcebergCreateTableOptions()).isEmpty();
  }

  // --- iceberg + metadata flag validation (Change 2) ---

  @Test
  void from_icebergWithDisabledTopicFlag_throws() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    raw.put(SNOWFLAKE_METADATA_TOPIC, "false");
    assertThatThrownBy(() -> SinkTaskConfig.from(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_AUTOCREATE_TABLE_TYPE)
        .hasMessageContaining("iceberg")
        .hasMessageContaining(SNOWFLAKE_METADATA_TOPIC);
  }

  @Test
  void from_icebergWithDisabledOffsetAndPartitionFlag_throws() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    raw.put(SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "false");
    assertThatThrownBy(() -> SinkTaskConfig.from(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_METADATA_OFFSET_AND_PARTITION);
  }

  @Test
  void from_icebergWithDisabledCreatetimeFlag_throws() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    raw.put(SNOWFLAKE_METADATA_CREATETIME, "false");
    assertThatThrownBy(() -> SinkTaskConfig.from(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_METADATA_CREATETIME);
  }

  @Test
  void from_icebergWithDisabledConnectorPushTimeFlag_throws() {
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    raw.put(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME, "false");
    assertThatThrownBy(() -> SinkTaskConfig.from(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME);
  }

  @Test
  void from_icebergWithAllMetadataEnabled_ok() {
    // All metadata flags are enabled by default; explicit confirmation must still succeed.
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    raw.put(SNOWFLAKE_METADATA_TOPIC, "true");
    raw.put(SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "true");
    raw.put(SNOWFLAKE_METADATA_CREATETIME, "true");
    raw.put(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME, "true");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.ICEBERG);
  }

  @Test
  void from_snowflakeTypeWithDisabledMetadata_ok() {
    // The metadata-mandated guard is Iceberg-only; standard Snowflake tables are unaffected.
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "snowflake");
    raw.put(SNOWFLAKE_METADATA_TOPIC, "false");
    raw.put(SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "false");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.SNOWFLAKE);
  }

  // --- structured headers ---

  @Test
  void from_icebergWithStructuredHeaders_ok() {
    // Structured headers + iceberg is allowed at config time. For v2 structured-OBJECT tables the
    // header values are coerced to String in conformIcebergMetadata (schema is
    // headers MAP(VARCHAR,VARCHAR)); v3/VARIANT keeps them nested. No config-time rejection.
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    raw.put(SNOWFLAKE_FEATURE_STRUCTURED_HEADERS, "true");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.ICEBERG);
  }

  @Test
  void from_icebergWithStructuredHeadersDisabled_ok() {
    // Default is false; explicit false must also succeed.
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    raw.put(SNOWFLAKE_FEATURE_STRUCTURED_HEADERS, "false");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.ICEBERG);
  }

  @Test
  void from_snowflakeTypeWithStructuredHeaders_ok() {
    // Structured headers are allowed for all table types (no config-time restriction).
    Map<String, String> raw = minimalConfig();
    raw.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "snowflake");
    raw.put(SNOWFLAKE_FEATURE_STRUCTURED_HEADERS, "true");
    SinkTaskConfig config = SinkTaskConfig.from(raw);
    assertThat(config.getAutocreatedTableType()).isEqualTo(TableType.SNOWFLAKE);
  }
}
