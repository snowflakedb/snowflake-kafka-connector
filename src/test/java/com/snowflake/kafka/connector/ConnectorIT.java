package com.snowflake.kafka.connector;

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
    SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE,
    SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
    SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
    SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME,
    SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
    SnowflakeSinkConnectorConfig.JVM_PROXY_PORT,
    SnowflakeSinkConnectorConfig.JVM_PROXY_HOST,
    SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
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

  private static void assertPropNoError(
      final Map<String, ConfigValue> validateMap, final String[] propArray) {
    List<String> propList = Arrays.asList(propArray);
    for (String prop : allProperties) {
      if (propList.contains(prop)) {
        assert validateMap.get(prop).errorMessages().isEmpty();
      } else {
        assert !validateMap.get(prop).errorMessages().isEmpty();
      }
    }
  }

  private static Map<String, ConfigValue> toValidateMap(final Map<String, String> config) {
    SnowflakeSinkConnector sinkConnector = new SnowflakeSinkConnector();
    Config result = sinkConnector.validate(config);
    return Utils.validateConfigToMap(result);
  }

  static Map<String, String> getErrorConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE, "");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "0");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, "0");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "-1");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL, "falseee");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC, "falseee");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "falseee");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME, "falseee");
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "jfsja,,");
    return config;
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
  public void testValidateErrorConfig() {
    Map<String, ConfigValue> validateMap = toValidateMap(getErrorConfig());
    // all single field validation has error
    assertPropNoError(
        validateMap,
        new String[] {
          SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
          SnowflakeSinkConnectorConfig.JVM_PROXY_PORT,
          SnowflakeSinkConnectorConfig.JVM_PROXY_HOST,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
        });
  }

  @Test
  public void testValidateEmptyConfig() {
    Map<String, ConfigValue> validateMap = toValidateMap(getEmptyConfig());
    assertPropHasError(
        validateMap,
        new String[] {
          SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE,
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
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL, "https://google.com");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SnowflakeSinkConnectorConfig.SNOWFLAKE_URL});
  }

  @Test
  public void testValidateErrorURLAccountConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(
        SnowflakeSinkConnectorConfig.SNOWFLAKE_URL, "wronggAccountt.snowflakecomputing.com:443");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap,
        new String[] {
          SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY
        });
  }

  @Test
  public void testValidateErrorUserConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER, "wrongUser");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap,
        new String[] {
          SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY
        });
  }

  @Test
  public void testValidateErrorPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, "wrongPassword");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  public void testValidateEmptyPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, "");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  public void testValidateNullPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY});
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testValidateNullOAuthClientIdConfig() {
    Map<String, String> config = getCorrectConfigWithOAuth();
    config.remove(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID});
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testValidateNullOAuthClientSecretConfig() {
    Map<String, String> config = getCorrectConfigWithOAuth();
    config.remove(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET});
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testValidateNullOAuthRefreshTokenConfig() {
    Map<String, String> config = getCorrectConfigWithOAuth();
    config.remove(SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap, new String[] {SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN});
  }

  @Test
  public void testValidateInvalidAuthenticator() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, "invalid_authenticator");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE});
  }

  @Test
  public void testValidateFilePasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, " ${file:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateConfigProviderPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, " ${configProvider:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateFilePassphraseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, " ${file:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateConfigProviderPassphraseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(
        SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, " ${configProvider:/");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {});
  }

  @Test
  public void testValidateErrorPassphraseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, "wrongPassphrase");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap,
        new String[] {
          SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
          SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
        });
  }

  @Test
  public void testValidateErrorDatabaseConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE, "wrongDatabase");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE});
  }

  @Test
  public void testValidateErrorSchemaConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA, "wrongSchema");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[] {SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA});
  }

  @Test
  public void testErrorProxyHostConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap,
        new String[] {
          SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, SnowflakeSinkConnectorConfig.JVM_PROXY_PORT
        });
  }

  @Test
  public void testErrorProxyPortConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "8080");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(
        validateMap,
        new String[] {
          SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, SnowflakeSinkConnectorConfig.JVM_PROXY_PORT
        });
  }

  @Test
  public void testProxyHostPortConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "8080");
    Utils.validateProxySettings(config);
  }

  @Test
  public void testErrorProxyUsernameConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "8080");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "user");

    Map<String, String> invalidConfigs = Utils.validateProxySettings(config);
    assert invalidConfigs.containsKey(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME);
  }

  @Test
  public void testErrorProxyPasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "8080");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "pass");

    Map<String, String> invalidConfigs = Utils.validateProxySettings(config);
    assert invalidConfigs.containsKey(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD);
  }

  @Test
  public void testProxyUsernamePasswordConfig() {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "admin");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "test");
    Utils.validateProxySettings(config);
  }

  @Test
  public void testConnectorComprehensive() {
    Map<String, String> config = getConf();
    SnowflakeSinkConnector sinkConnector = new SnowflakeSinkConnector();
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
    SnowflakeSinkConnector sinkConnector = new SnowflakeSinkConnector();
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
