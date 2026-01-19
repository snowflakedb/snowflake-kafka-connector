package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
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
    config.put(KafkaConnectorConfigParams.NAME, name);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withTopics(String topics) {
    config.put(KafkaConnectorConfigParams.TOPICS, topics);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withUrl(String url) {
    config.put(SNOWFLAKE_URL_NAME, url);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withDatabase(String database) {
    config.put(SNOWFLAKE_DATABASE_NAME, database);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withSchema(String schema) {
    config.put(SNOWFLAKE_SCHEMA_NAME, schema);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withUser(String user) {
    config.put(SNOWFLAKE_USER_NAME, user);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withPrivateKey(String privateKey) {
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, privateKey);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withRole(String role) {
    config.put(SNOWFLAKE_ROLE_NAME, role);
    return this;
  }

  public SnowflakeSinkConnectorConfigBuilder withoutRole() {
    config.remove(SNOWFLAKE_ROLE_NAME);
    return this;
  }

  public Map<String, String> build() {
    return config;
  }
}
