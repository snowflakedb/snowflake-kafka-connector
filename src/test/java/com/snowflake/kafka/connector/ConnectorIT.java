package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JVM_PROXY_HOST;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JVM_PROXY_PORT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_ROLE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_URL;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_USER;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP;
import static com.snowflake.kafka.connector.Utils.NAME;
import static com.snowflake.kafka.connector.Utils.TASK_ID;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.TestUtils.getConf;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfWithOAuth;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Ignore;
import org.junit.Test;

public class ConnectorIT {
  static final String allPropertiesList[] = {
    SNOWFLAKE_URL,
    SNOWFLAKE_USER,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_METADATA_ALL,
    SNOWFLAKE_METADATA_TOPIC,
    SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
    SNOWFLAKE_METADATA_CREATETIME,
    TOPICS_TABLES_MAP,
    SNOWFLAKE_PRIVATE_KEY,
    JVM_PROXY_PORT,
    JVM_PROXY_HOST,
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
  };

  static final Set<String> allProperties = new HashSet<>(Arrays.asList(allPropertiesList));

  private static void assertPropHasError(
      final Map<String, ConfigValue> validateMap, final String[] propArray) {
    List<String> propList = Arrays.asList(propArray);
    for (String prop : allProperties) {
      if (propList.contains(prop)) {
        assert !validateMap.get(prop).errorMessages().isEmpty();
      } else {
        assert validateMap.get(prop).errorMessages().isEmpty();
      }
    }
  }

  private static Map<String, ConfigValue> toValidateMap(final Map<String, String> config) {
    SnowflakeStreamingSinkConnector sinkConnector = new SnowflakeStreamingSinkConnector();
    Config result = sinkConnector.validate(config);
    return Utils.validateConfigToMap(result);
  }

  static Map<String, String> getEmptyConfig() {
    Map<String, String> config = new HashMap<>();
    return config;
  }

  static Map<String, String> getCorrectConfig() {
    Map<String, String> config = getConf();
    config.remove(Utils.SF_WAREHOUSE);
    config.remove(Utils.NAME);
    config.remove(TASK_ID);
    return config;
  }

  static Map<String, String> getCorrectConfigWithOAuth() {
    Map<String, String> config = getConfWithOAuth();
    config.remove(Utils.SF_WAREHOUSE);
    config.remove(Utils.NAME);
    config.remove(TASK_ID);
    return config;
  }

  @Test
  public void testValidateErrorConfigImproved() {
    // Given: a configuration with intentionally invalid values
    Map<String, String> config = new HashMap<>();
    config.put(SNOWFLAKE_URL, "");
    config.put(SNOWFLAKE_USER, "");
    config.put(SNOWFLAKE_DATABASE, "");
    config.put(SNOWFLAKE_PRIVATE_KEY, "");
    config.put(SNOWFLAKE_ROLE, "");
    config.put(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, "");
    config.put(SNOWFLAKE_SCHEMA, "");
    config.put(SNOWFLAKE_METADATA_TOPIC, "falseee");
    config.put(SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "falseee");
    config.put(SNOWFLAKE_METADATA_CREATETIME, "falseee");
    config.put(TOPICS_TABLES_MAP, "jfsja,,");
    Map<String, ConfigValue> validateMap = toValidateMap(config);

    // Optional properties that should NOT have errors (even when empty or missing)
    Set<String> optionalProperties =
        new HashSet<>(
            Arrays.asList(
                SNOWFLAKE_PRIVATE_KEY,
                JVM_PROXY_PORT,
                JVM_PROXY_HOST,
                SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
                SNOWFLAKE_METADATA_ALL));

    // Required properties or properties with format validation that SHOULD have errors
    Set<String> requiredOrValidatedProperties =
        new HashSet<>(
            Arrays.asList(
                SNOWFLAKE_URL, // empty string - required
                SNOWFLAKE_USER, // empty string - required
                SNOWFLAKE_DATABASE, // empty string - required
                SNOWFLAKE_SCHEMA, // empty string - required
                SNOWFLAKE_METADATA_TOPIC, // invalid boolean "falseee"
                SnowflakeSinkConnectorConfig
                    .SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, // invalid boolean "falseee"
                SnowflakeSinkConnectorConfig
                    .SNOWFLAKE_METADATA_CREATETIME, // invalid boolean "falseee"
                TOPICS_TABLES_MAP // invalid format "jfsja,,"
                ));

    // Assert: optional properties should have NO errors
    for (String optionalProp : optionalProperties) {
      ConfigValue configValue = validateMap.get(optionalProp);
      assert configValue != null
          : String.format("Property '%s' not found in validation results", optionalProp);
      assert configValue.errorMessages().isEmpty()
          : String.format(
              "Optional property '%s' should not have errors, but has: %s",
              optionalProp, configValue.errorMessages());
    }

    // Assert: required/validated properties SHOULD have errors
    for (String requiredProp : requiredOrValidatedProperties) {
      ConfigValue configValue = validateMap.get(requiredProp);
      assert configValue != null
          : String.format("Property '%s' not found in validation results", requiredProp);
      assert !configValue.errorMessages().isEmpty()
          : String.format(
              "Required/validated property '%s' should have validation errors but has none. "
                  + "Current value: '%s'",
              requiredProp, configValue.value());
    }
  }

  @Test
  public void testValidateEmptyConfig() {
    Map<String, ConfigValue> validateMap = toValidateMap(getEmptyConfig());
    assertPropHasError(
        validateMap,
        new String[] {
          SNOWFLAKE_USER, SNOWFLAKE_URL, SNOWFLAKE_SCHEMA, SNOWFLAKE_DATABASE,
        });
  }

  @Test
  public void testValidateCorrectConfig() {
    Map<String, ConfigValue> validateMap = toValidateMap(getCorrectConfig());
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testValidateCorrectConfigWithOAuth() {
    Map<String, ConfigValue> validateMap = toValidateMap(getCorrectConfigWithOAuth());
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateErrorURLFormatConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_URL, "https://google.com");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SNOWFLAKE_URL});
  }

  @Test
  public void testValidateErrorURLAccountConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_URL, "wronggAccountt.snowflakecomputing.com:443");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SNOWFLAKE_USER, SNOWFLAKE_URL, SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  public void testValidateErrorUserConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_USER, "wrongUser");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SNOWFLAKE_USER, SNOWFLAKE_URL, SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  public void testValidateErrorPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_PRIVATE_KEY, "wrongPassword");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  public void testValidateEmptyPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_PRIVATE_KEY, "");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  public void testValidateNullPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.remove(SNOWFLAKE_PRIVATE_KEY);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testValidateNullOAuthClientIdConfig() {
    Map<String, String> config = getCorrectConfigWithOAuth();
    config.remove(OAUTH_CLIENT_ID);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {OAUTH_CLIENT_ID});
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testValidateNullOAuthClientSecretConfig() {
    Map<String, String> config = getCorrectConfigWithOAuth();
    config.remove(OAUTH_CLIENT_SECRET);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {OAUTH_CLIENT_SECRET});
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testValidateNullOAuthRefreshTokenConfig() {
    Map<String, String> config = getCorrectConfigWithOAuth();
    config.remove(OAUTH_REFRESH_TOKEN);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {OAUTH_REFRESH_TOKEN});
  }

  @Test
  public void testValidateInvalidAuthenticator() {
    Map<String, String> config = getCorrectConfig();
    config.put(AUTHENTICATOR_TYPE, "invalid_authenticator");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {AUTHENTICATOR_TYPE});
  }

  @Test
  public void testValidateFilePasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_PRIVATE_KEY, " ${file:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateConfigProviderPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_PRIVATE_KEY, " ${configProvider:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateFilePassphraseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, " ${file:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateConfigProviderPassphraseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, " ${configProvider:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateErrorPassphraseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, "wrongPassphrase");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SNOWFLAKE_PRIVATE_KEY, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE});
  }

  @Test
  public void testValidateErrorDatabaseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_DATABASE, "wrongDatabase");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SNOWFLAKE_DATABASE});
  }

  @Test
  public void testValidateErrorSchemaConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SNOWFLAKE_SCHEMA, "wrongSchema");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SNOWFLAKE_SCHEMA});
  }

  @Test
  public void testErrorProxyHostConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(JVM_PROXY_HOST, "localhost");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {JVM_PROXY_HOST, JVM_PROXY_PORT});
  }

  @Test
  public void testErrorProxyPortConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(JVM_PROXY_PORT, "8080");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {JVM_PROXY_HOST, JVM_PROXY_PORT});
  }

  @Test
  public void testProxyHostPortConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(JVM_PROXY_HOST, "localhost");
    config.put(JVM_PROXY_PORT, "8080");
    Utils.validateProxySettings(config);
  }

  @Test
  public void testErrorProxyUsernameConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(JVM_PROXY_HOST, "localhost");
    config.put(JVM_PROXY_PORT, "8080");
    config.put(JVM_PROXY_USERNAME, "user");

    Map<String, String> invalidConfigs = Utils.validateProxySettings(config);
    assert invalidConfigs.containsKey(JVM_PROXY_USERNAME);
  }

  @Test
  public void testErrorProxyPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(JVM_PROXY_HOST, "localhost");
    config.put(JVM_PROXY_PORT, "8080");
    config.put(JVM_PROXY_PASSWORD, "pass");

    Map<String, String> invalidConfigs = Utils.validateProxySettings(config);
    assert invalidConfigs.containsKey(JVM_PROXY_PASSWORD);
  }

  @Test
  public void testProxyUsernamePasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(JVM_PROXY_HOST, "localhost");
    config.put(JVM_PROXY_PORT, "3128");
    config.put(JVM_PROXY_USERNAME, "admin");
    config.put(JVM_PROXY_PASSWORD, "test");
    Utils.validateProxySettings(config);
  }

  @Test
  public void testConnectorComprehensive() {
    Map<String, String> config = getConf();
    SnowflakeStreamingSinkConnector sinkConnector = new SnowflakeStreamingSinkConnector();
    sinkConnector.start(config);
    assert sinkConnector.taskClass().equals(SnowflakeSinkTask.class);
    List<Map<String, String>> taskConfigs = sinkConnector.taskConfigs(2);
    assert taskConfigs.get(0).get(TASK_ID).equals("0");
    assert taskConfigs.get(0).get(NAME).equals(TEST_CONNECTOR_NAME);
    assert taskConfigs.get(1).get(TASK_ID).equals("1");
    sinkConnector.stop();
    assert sinkConnector.version().equals(Utils.VERSION);
  }

  @Test
  public void testConnectorComprehensiveNegative() throws Exception {
    Map<String, String> config = getConf();
    SnowflakeStreamingSinkConnector sinkConnector = new SnowflakeStreamingSinkConnector();
    ExecutorService testThread = Executors.newSingleThreadExecutor();
    testThread.submit(
        () -> {
          // After 10 minutes this thread will throw error. 10 minutes is too long
          // for this test, so kill the thread after 6 seconds, which should have
          // covered enough lines.
          sinkConnector.taskConfigs(2);
        });
    Thread.sleep(6000);
    testThread.shutdownNow();
  }
}
