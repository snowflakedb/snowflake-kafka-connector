package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ICEBERG_ENABLED;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JVM_PROXY_HOST;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JVM_PROXY_PORT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.NAME;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_ROLE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_URL;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_USER;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP;
import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_HOST;
import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_USER;
import static com.snowflake.kafka.connector.Utils.HTTP_NON_PROXY_HOSTS;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_HOST;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_PORT;
import static com.snowflake.kafka.connector.Utils.HTTP_USE_PROXY;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfForStreaming;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.config.TopicToTableModeExtractor;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class ConnectorConfigValidatorTest {

  // subset of valid community converters
  public static final List<Converter> COMMUNITY_CONVERTER_SUBSET =
      Arrays.asList(
          new org.apache.kafka.connect.storage.StringConverter(),
          new org.apache.kafka.connect.json.JsonConverter(),
          new io.confluent.connect.avro.AvroConverter());

  private final ConnectorConfigValidator connectorConfigValidator =
      new DefaultConnectorConfigValidator(new DefaultStreamingConfigValidator());

  public static Stream<Arguments> validConfigs() {
    return Stream.of(
        Arguments.of(SnowflakeSinkConnectorConfigBuilder.streamingConfig().build()),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.streamingConfig()
                .withUseUserDefinedDatabaseObjects(true)
                .build()),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.streamingConfig()
                .withSchematizationEnabled(false)
                .build()));
  }

  static Stream<Arguments> invalidConfigs() {
    return Stream.of(
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.streamingConfig()
                .withSchematizationEnabled(true)
                .build(),
            "Schematization is not yet supported"),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.icebergConfig()
                .withSchematizationEnabled(true)
                .build(),
            "snowflake.streaming.iceberg.enabled"),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.icebergConfig()
                .withSchematizationEnabled(false)
                .build(),
            "snowflake.streaming.iceberg.enabled"),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.icebergConfig().build(),
            "snowflake.streaming.iceberg.enabled"));
  }

  @ParameterizedTest(name = "Valid config: {0}")
  @MethodSource("validConfigs")
  public void shouldValidateCorrectConfig(Map<String, String> config) {
    // no exception thrown
    connectorConfigValidator.validateConfig(config);
  }

  @ParameterizedTest(name = "Invalid config: {0}")
  @MethodSource("invalidConfigs")
  void shouldReturnErrorOnInvalidConfig(final Map<String, String> config, String errorKey) {
    //        Map<String, String> invalidParameters = validator.validate(config);
    //        assertThat(invalidParameters).containsKey(errorKey);
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(errorKey);
  }

  @Test
  public void testConfig() {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testConfig_ConvertedInvalidAppName() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withName("testConfig.snowflake-connector")
            .build();

    Utils.convertAppName(config);

    connectorConfigValidator.validateConfig(config);
  }

  @ParameterizedTest
  @CsvSource({
    NAME,
    SNOWFLAKE_URL,
    SNOWFLAKE_USER,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_ROLE
  })
  public void shouldThrowExForEmptyProperty(String prop) {
    Map<String, String> config = getConfig();
    config.remove(prop);
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(prop);
  }

  @Test
  public void testCorrectProxyHost() {
    Map<String, String> config = getConfig();
    config.put(JVM_PROXY_HOST, "127.0.0.1");
    config.put(JVM_PROXY_PORT, "3128");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testEmptyPort() {
    Map<String, String> config = getConfig();
    config.put(JVM_PROXY_HOST, "127.0.0.1");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(JVM_PROXY_HOST);
  }

  @Test
  public void testEmptyHost() {
    Map<String, String> config = getConfig();
    config.put(JVM_PROXY_PORT, "3128");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(JVM_PROXY_PORT);
  }

  @Test
  public void testNonProxyHosts() {
    String oldNonProxyHosts =
        (System.getProperty(HTTP_NON_PROXY_HOSTS) != null)
            ? System.getProperty(HTTP_NON_PROXY_HOSTS)
            : null;

    System.setProperty(HTTP_NON_PROXY_HOSTS, "host1.com|host2.com|localhost");
    Map<String, String> config = getConfig();
    config.put(JVM_PROXY_HOST, "127.0.0.1");
    config.put(JVM_PROXY_PORT, "3128");
    config.put(
        SnowflakeSinkConnectorConfig.JVM_NON_PROXY_HOSTS,
        "*.snowflakecomputing.com|*.amazonaws.com");
    Utils.enableJVMProxy(config);
    String mergedNonProxyHosts = System.getProperty(HTTP_NON_PROXY_HOSTS);
    Assert.assertTrue(
        mergedNonProxyHosts.equals(
            "host1.com|host2.com|localhost|*.snowflakecomputing.com|*.amazonaws.com"));

    if (oldNonProxyHosts != null) {
      System.setProperty(HTTP_NON_PROXY_HOSTS, oldNonProxyHosts);
    } else {
      System.clearProperty(HTTP_NON_PROXY_HOSTS);
    }

    // clear properties to prevent other tests from failing
    System.clearProperty(HTTP_USE_PROXY);
    System.clearProperty(HTTP_PROXY_HOST);
    System.clearProperty(HTTP_PROXY_PORT);
    System.clearProperty(HTTPS_PROXY_HOST);
    System.clearProperty(HTTPS_PROXY_PORT);
    System.clearProperty(HTTPS_PROXY_USER);
    System.clearProperty(HTTPS_PROXY_PASSWORD);
  }

  @Test
  public void testIllegalTopicMap() {
    Map<String, String> config = getConfig();
    config.put(TOPICS_TABLES_MAP, "$@#$#@%^$12312");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(TOPICS_TABLES_MAP);
  }

  @Test
  public void testIllegalTableName() {
    Map<String, String> config = getConfig();
    config.put(TOPICS_TABLES_MAP, "topic1:!@#@!#!@");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .matches(
            ex ->
                ((SnowflakeKafkaConnectorException) ex)
                    .getCode()
                    .equals(SnowflakeErrors.ERROR_0021.getCode()));
  }

  @Test
  public void testDuplicatedTopic() {
    Map<String, String> config = getConfig();
    config.put(TOPICS_TABLES_MAP, "topic1:table1,topic1:table2");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .matches(
            ex ->
                ((SnowflakeKafkaConnectorException) ex)
                    .getCode()
                    .equals(SnowflakeErrors.ERROR_0021.getCode()));
  }

  @Test
  public void testDuplicatedTableName() {
    Map<String, String> config = getConfig();
    config.put(TOPICS_TABLES_MAP, "topic1:table1,topic2:table1");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testNameMapCovered() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS, "!@#,$%^,test");
    config.put(TOPICS_TABLES_MAP, "!@#:table1,$%^:table2");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testTopic2TableCorrectlyDeterminesMode() {
    Map<String, String> config = getConfig();
    config.put(TOPICS_TABLES_MAP, "src1:target1,src2:target2,src3:target1");
    connectorConfigValidator.validateConfig(config);
    Map<String, String> topic2Table = Utils.parseTopicToTableMap(config.get(TOPICS_TABLES_MAP));
    assertThat(TopicToTableModeExtractor.determineTopic2TableMode(topic2Table, "src1"))
        .isEqualTo(TopicToTableModeExtractor.Topic2TableMode.MANY_TOPICS_SINGLE_TABLE);
    assertThat(TopicToTableModeExtractor.determineTopic2TableMode(topic2Table, "src2"))
        .isEqualTo(TopicToTableModeExtractor.Topic2TableMode.SINGLE_TOPIC_SINGLE_TABLE);
    assertThat(TopicToTableModeExtractor.determineTopic2TableMode(topic2Table, "src3"))
        .isEqualTo(TopicToTableModeExtractor.Topic2TableMode.MANY_TOPICS_SINGLE_TABLE);
  }

  @Test
  public void testKafkaProviderConfigValue_valid_null() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, null);
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testKafkaProviderConfigValue_valid_empty() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testKafkaProviderConfigValue_valid_provider() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "self_hosted");
    connectorConfigValidator.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "CONFLUENT");
    connectorConfigValidator.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "UNKNOWN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testKafkaProviderConfigValue_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "Something_which_is_not_supported");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG);
  }

  @Test
  public void testBehaviorOnNullValuesConfig_valid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "IGNORE");
    connectorConfigValidator.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "DEFAULT");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testBehaviorOnNullValuesConfig_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "INVALID");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG);
  }

  @Test
  public void testJMX_valid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JMX_OPT, "true");
    connectorConfigValidator.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.JMX_OPT, "False");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testJMX_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JMX_OPT, "INVALID");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.JMX_OPT);
  }

  @Test
  public void testIngestionTypeConfig_valid_value_snowpipe() {
    Map<String, String> config = getConfig();

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_valid_value_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_invalid_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(Utils.SF_ROLE, "");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(Utils.SF_ROLE);
  }

  /** These error tests are not going to enforce errors if they are not passed as configs. */
  @Test
  public void testErrorTolerance_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);

    config.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.NONE.toString());
    connectorConfigValidator.validateConfig(config);

    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testErrorTolerance_DisallowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_TOLERANCE_CONFIG, "INVALID");

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("snowflake.ingestion.method");
  }

  @Test
  public void testErrorLog_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_LOG_ENABLE_CONFIG, "true");

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);

    config.put(ERRORS_LOG_ENABLE_CONFIG, "FALSE");
    connectorConfigValidator.validateConfig(config);

    config.put(ERRORS_LOG_ENABLE_CONFIG, "TRUE");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testErrorLog_DisallowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_LOG_ENABLE_CONFIG, "INVALID");

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG);
  }

  @Test
  public void testValidKeyAndValueConvertersForStreamingSnowpipe() {
    Map<String, String> config = getConfig();

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");

    COMMUNITY_CONVERTER_SUBSET.forEach(
        converter -> {
          config.put(
              SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD,
              converter.getClass().toString());
          connectorConfigValidator.validateConfig(config);
        });

    COMMUNITY_CONVERTER_SUBSET.forEach(
        converter -> {
          config.put(
              SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
              converter.getClass().toString());
          connectorConfigValidator.validateConfig(config);
        });
  }

  @ParameterizedTest
  @CsvSource({
    SNOWPIPE_STREAMING_MAX_CLIENT_LAG + ", 1",
    SNOWPIPE_STREAMING_MAX_CLIENT_LAG + ", 3",
    SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES + ", 10"
  })
  public void shouldNotThrowExceptionForProperStreamingClientPropsValue(String prop, String value) {
    // GIVEN
    Map<String, String> config = getConfig();

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(prop, value);

    // WHEN/THEN
    connectorConfigValidator.validateConfig(config);
  }

  @ParameterizedTest
  @CsvSource({
    SNOWPIPE_STREAMING_MAX_CLIENT_LAG + ", fdf",
    SNOWPIPE_STREAMING_MAX_CLIENT_LAG + ", 10 dsada",
    SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES + ", fdf",
    SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES + ", 10 dsada",
    SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES + ", fdf",
    SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES + ", 10 dsada"
  })
  public void shouldThrowExceptionForInvalidStreamingClientPropsValue(String prop, String value) {
    // GIVEN
    Map<String, String> config = getConfig();

    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(prop, value);

    // WHEN/THEN
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(prop);
  }

  @Test
  public void testSchematizationWithUnsupportedConverter() {
    Map<String, String> config = getConfig();
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("org.apache.kafka.connect.storage.StringConverter");
  }

  @Test
  public void testDisabledSchematizationWithUnsupportedConverter() {
    Map<String, String> config = getConfig();
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "false");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testStreamingProviderOverrideConfig_validWithSnowpipeStreaming() {
    Map<String, String> config = getConfig();
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "a:b,c:d,e:100,f:true");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testInvalidEmptyConfig() {
    Map<String, String> config = new HashMap<>();
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SNOWFLAKE_DATABASE)
        .hasMessageContaining(SNOWFLAKE_SCHEMA)
        .hasMessageContaining(SNOWFLAKE_PRIVATE_KEY)
        .hasMessageContaining(SNOWFLAKE_USER)
        .hasMessageContaining(NAME)
        .hasMessageContaining(SNOWFLAKE_ROLE)
        .hasMessageContaining(SNOWFLAKE_URL);
  }

  // removes each of the following params iteratively to test if the log/exception has all the
  // expected removed params
  @Test
  public void testMultipleInvalidConfigs() {
    List<String> emptyParams =
        Arrays.asList(
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_SCHEMA,
            SNOWFLAKE_PRIVATE_KEY,
            SNOWFLAKE_USER,
            NAME,
            SNOWFLAKE_ROLE,
            SNOWFLAKE_URL);
    List<String> paramsToRemove = new ArrayList<String>();

    for (String param : emptyParams) {
      paramsToRemove.add(param);
      this.invalidConfigRunner(paramsToRemove);
    }
  }

  @Test
  public void testOAuthAuthenticator() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, Utils.OAUTH);
    config.put(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID, "client_id");
    config.put(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET, "client_secret");
    config.put(SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN, "refresh_token");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testInvalidAuthenticator() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, "invalid_authenticator");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE);
  }

  @Test
  public void testExternalOAuthConfig() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(Utils.OAUTH)
            .withOauthClientId("client_id")
            .withOauthClientSecret("client_secret")
            .withOauthRefreshToken("refresh_token")
            .withOauthTokenEndpoint("token_endpoint")
            .build();
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testEmptyClientId() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, Utils.OAUTH);
    config.put(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET, "client_secret");
    config.put(SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN, "refresh_token");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID);
  }

  @Test
  public void testEmptyClientSecret() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, Utils.OAUTH);
    config.put(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID, "client_id");
    config.put(SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN, "refresh_token");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET);
  }

  @Test
  public void testEmptyRefreshToken() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, Utils.OAUTH);
    config.put(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID, "client_id");
    config.put(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET, "client_secret");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN);
  }

  @Test
  public void shouldValidateSSv2Config() {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();

    assertThatCode(() -> connectorConfigValidator.validateConfig(config))
        .doesNotThrowAnyException();
  }

  @Test
  public void shouldThrowExceptionWhenRoleNotDefinedForSSv2() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().withoutRole().build();

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SNOWFLAKE_ROLE);
  }

  @Test
  public void shouldThrowExceptionWhenBothSSv2AndIcebergEnabled() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().withIcebergEnabled().build();

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("Ingestion to Iceberg table is currently unsupported")
        .hasMessageContaining(ICEBERG_ENABLED);
  }

  @Test
  public void shouldValidateSSv2WithoutIceberg() {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();

    assertThatCode(() -> connectorConfigValidator.validateConfig(config))
        .doesNotThrowAnyException();
  }

  private void invalidConfigRunner(List<String> paramsToRemove) {
    Map<String, String> config = getConfig();
    for (String configParam : paramsToRemove) {
      config.remove(configParam);
    }

    try {
      connectorConfigValidator.validateConfig(config);
    } catch (SnowflakeKafkaConnectorException exception) {
      for (String configParam : paramsToRemove) {
        assert exception.getMessage().contains(configParam);
      }
    }
  }

  @Test
  public void testSchematizationWithStreamingV2Enabled_shouldFail() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSchematizationEnabled(true)
            .build();

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(ENABLE_SCHEMATIZATION_CONFIG)
        .hasMessageContaining(
            "Schematization is not yet supported with Snowpipe Streaming: High-Performance"
                + " Architecture");
  }

  @Test
  public void testSchematizationDisabledWithStreamingV2Enabled_shouldPass() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSchematizationEnabled(false)
            .build();

    assertThatCode(() -> connectorConfigValidator.validateConfig(config))
        .doesNotThrowAnyException();
  }

  @Test
  public void test_snowpipe_streaming_use_user_defined_database_objects() {
    Map<String, String> config = getConfForStreaming();
    connectorConfigValidator.validateConfig(config);
    config.put(SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS, "False");
    connectorConfigValidator.validateConfig(config);
    config.put(SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS, "True");
    connectorConfigValidator.validateConfig(config);
    config.put(SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS, "INVALID");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS);
  }

  @Test
  public void
      test_snowpipe_streaming_use_user_defined_database_objects_invalid_with_schematization_enabled() {
    Map<String, String> config = getConfForStreaming();
    config.put(SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS, "true");
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS)
        .hasMessageContaining(ENABLE_SCHEMATIZATION_CONFIG)
        .hasMessageContaining("mutually exclusive");
  }
}
