package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.Utils.SF_DATABASE;
import static com.snowflake.kafka.connector.Utils.SF_ROLE;
import static com.snowflake.kafka.connector.Utils.SF_SCHEMA;
import static com.snowflake.kafka.connector.Utils.SF_URL;
import static com.snowflake.kafka.connector.Utils.SF_USER;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a builder class for the connector config. For now it returns map. Let's change it to a
 * more convenient abstraction when we have it.
 */
public class SnowflakeSinkConnectorConfigBuilder {

  private final Map<String, String> config = new HashMap<String, String>();

  private SnowflakeSinkConnectorConfigBuilder() {}

  public static SnowflakeSinkConnectorConfigBuilder streamingConfig() {
    return commonRequiredFields();
  }

  public static SnowflakeSinkConnectorConfigBuilder icebergConfig() {
    return commonRequiredFields().withIcebergEnabled().withSchematizationEnabled(true);
  }

  private static SnowflakeSinkConnectorConfigBuilder commonRequiredFields() {
    return new SnowflakeSinkConnectorConfigBuilder()
        .withName("test")
        .withTopics("topic1,topic2")
        .withUrl("https://testaccount.snowflake.com:443")
        .withSchema("testSchema")
        .withDatabase("testDatabase")
        .withUser("userName")
        .withPrivateKey("fdsfsdfsdfdsfdsrqwrwewrwrew42314424")
        .withRole("role");
  }

  public SnowflakeSinkConnectorConfigBuilder withName(String name) {
    config.put(Utils.NAME, name);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withTopics(String topics) {
    config.put(SnowflakeSinkConnectorConfig.TOPICS, topics);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withUseUserDefinedDatabaseObjects(boolean value) {
    config.put(
        SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS,
        "" + value);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withUrl(String url) {
    config.put(SF_URL, url);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withDatabase(String database) {
    config.put(SF_DATABASE, database);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withSchema(String schema) {
    config.put(SF_SCHEMA, schema);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withUser(String user) {
    config.put(SF_USER, user);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withPrivateKey(String privateKey) {
    config.put(Utils.SF_PRIVATE_KEY, privateKey);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withIcebergEnabled() {
    config.put(SnowflakeSinkConnectorConfig.ICEBERG_ENABLED, "true");
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withRole(String role) {
    config.put(SF_ROLE, role);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withoutRole() {
    config.remove(SF_ROLE);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withSchematizationEnabled(boolean enabled) {
    config.put(ENABLE_SCHEMATIZATION_CONFIG, Boolean.toString(enabled));
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withAuthenticator(String value) {
    config.put(Utils.SF_AUTHENTICATOR, value);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withOauthClientId(String value) {
    config.put(Utils.SF_OAUTH_CLIENT_ID, value);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withOauthClientSecret(String value) {
    config.put(Utils.SF_OAUTH_CLIENT_SECRET, value);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withOauthRefreshToken(String value) {
    config.put(Utils.SF_OAUTH_REFRESH_TOKEN, value);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withOauthTokenEndpoint(String value) {
    config.put(Utils.SF_OAUTH_TOKEN_ENDPOINT, value);
    return this;
  }

  public Map<String, String> build() {
    return config;
  }
}
