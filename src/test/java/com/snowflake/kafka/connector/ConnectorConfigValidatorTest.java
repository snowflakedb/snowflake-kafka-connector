package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_USER;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_USE_PROXY;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JVM_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JVM_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_ICEBERG_ENABLED;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfig;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
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
          new org.apache.kafka.connect.json.JsonConverter(),
          new io.confluent.connect.avro.AvroConverter());

  private final ConnectorConfigValidator connectorConfigValidator =
      new DefaultConnectorConfigValidator(new DefaultStreamingConfigValidator());

  public static Stream<Arguments> validConfigs() {
    return Stream.of(
        Arguments.of(SnowflakeSinkConnectorConfigBuilder.streamingConfig().build()),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.streamingConfig()
                .withSchematizationEnabled(true)
                .build()));
  }

  static Stream<Arguments> invalidConfigs() {
    return Stream.of(
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.streamingConfig()
                .withSchematizationEnabled(false)
                .build(),
            "Schematization must be enabled"),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.icebergConfig().withIcebergEnabled().build(),
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
    SNOWFLAKE_URL_NAME,
    SNOWFLAKE_USER_NAME,
    SNOWFLAKE_DATABASE_NAME,
    SNOWFLAKE_SCHEMA_NAME,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_ROLE_NAME
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
        KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS, "*.snowflakecomputing.com|*.amazonaws.com");
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
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, "$@#$#@%^$12312");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SNOWFLAKE_TOPICS2TABLE_MAP);
  }

  @Test
  public void testIllegalTableName() {
    Map<String, String> config = getConfig();
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, "topic1:!@#@!#!@");
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
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, "topic1:table1,topic1:table2");
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
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, "topic1:table1,topic2:table1");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testNameMapCovered() {
    Map<String, String> config = getConfig();
    config.put(KafkaConnectorConfigParams.TOPICS, "!@#,$%^,test");
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, "!@#:table1,$%^:table2");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testBehaviorOnNullValuesConfig_valid_value() {
    Map<String, String> config = getConfig();
    config.put(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES, "IGNORE");
    connectorConfigValidator.validateConfig(config);

    config.put(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES, "DEFAULT");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testBehaviorOnNullValuesConfig_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES, "INVALID");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES);
  }

  @Test
  public void testJMX_valid_value() {
    Map<String, String> config = getConfig();
    config.put(KafkaConnectorConfigParams.JMX_OPT, "true");
    connectorConfigValidator.validateConfig(config);

    config.put(KafkaConnectorConfigParams.JMX_OPT, "False");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testJMX_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(KafkaConnectorConfigParams.JMX_OPT, "INVALID");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(KafkaConnectorConfigParams.JMX_OPT);
  }

  @Test
  public void testIngestionTypeConfig_valid_value_snowpipe() {
    Map<String, String> config = getConfig();

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_valid_value_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_invalid_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME);
  }

  /** These error tests are not going to enforce errors if they are not passed as configs. */
  @Test
  public void testErrorTolerance_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_TOLERANCE_CONFIG, ConnectorConfigTools.ErrorTolerance.ALL.toString());

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);

    config.put(ERRORS_TOLERANCE_CONFIG, ConnectorConfigTools.ErrorTolerance.NONE.toString());
    connectorConfigValidator.validateConfig(config);

    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testErrorTolerance_DisallowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_TOLERANCE_CONFIG, "INVALID");

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(ERRORS_TOLERANCE_CONFIG);
  }

  @Test
  public void testErrorLog_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_LOG_ENABLE_CONFIG, "true");

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
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

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG);
  }

  @Test
  public void testValidKeyAndValueConvertersForStreamingSnowpipe() {
    Map<String, String> config = getConfig();

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");

    COMMUNITY_CONVERTER_SUBSET.forEach(
        converter -> {
          config.put(KafkaConnectorConfigParams.KEY_CONVERTER, converter.getClass().toString());
          connectorConfigValidator.validateConfig(config);
        });

    COMMUNITY_CONVERTER_SUBSET.forEach(
        converter -> {
          config.put(KafkaConnectorConfigParams.VALUE_CONVERTER, converter.getClass().toString());
          connectorConfigValidator.validateConfig(config);
        });
  }

  @ParameterizedTest
  @CsvSource({
    SNOWFLAKE_STREAMING_MAX_CLIENT_LAG + ", 1",
    SNOWFLAKE_STREAMING_MAX_CLIENT_LAG + ", 3"
  })
  public void shouldNotThrowExceptionForProperStreamingClientPropsValue(String prop, String value) {
    // GIVEN
    Map<String, String> config = getConfig();

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    config.put(prop, value);

    // WHEN/THEN
    connectorConfigValidator.validateConfig(config);
  }

  @ParameterizedTest
  @CsvSource({
    SNOWFLAKE_STREAMING_MAX_CLIENT_LAG + ", fdf",
    SNOWFLAKE_STREAMING_MAX_CLIENT_LAG + ", 10 dsada"
  })
  public void shouldThrowExceptionForInvalidStreamingClientPropsValue(String prop, String value) {
    // GIVEN
    Map<String, String> config = getConfig();

    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    config.put(prop, value);

    // WHEN/THEN
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(prop);
  }

  @Test
  public void testSchematizationWithUnsupportedConverter() {
    Map<String, String> config = getConfig();
    config.put(SNOWFLAKE_ENABLE_SCHEMATIZATION, "true");
    config.put(
        KafkaConnectorConfigParams.VALUE_CONVERTER,
        "org.apache.kafka.connect.storage.StringConverter");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("org.apache.kafka.connect.storage.StringConverter");
  }

  @Test
  public void testStreamingProviderOverrideConfig_validWithSnowpipeStreaming() {
    Map<String, String> config = getConfig();
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ACCOUNTADMIN");
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "a:b,c:d,e:100,f:true");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testInvalidEmptyConfig() {
    Map<String, String> config = new HashMap<>();
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SNOWFLAKE_DATABASE_NAME)
        .hasMessageContaining(SNOWFLAKE_SCHEMA_NAME)
        .hasMessageContaining(SNOWFLAKE_PRIVATE_KEY)
        .hasMessageContaining(SNOWFLAKE_USER_NAME)
        .hasMessageContaining(NAME)
        .hasMessageContaining(SNOWFLAKE_ROLE_NAME)
        .hasMessageContaining(SNOWFLAKE_URL_NAME);
  }

  // removes each of the following params iteratively to test if the log/exception has all the
  // expected removed params
  @Test
  public void testMultipleInvalidConfigs() {
    List<String> emptyParams =
        Arrays.asList(
            SNOWFLAKE_DATABASE_NAME,
            SNOWFLAKE_SCHEMA_NAME,
            SNOWFLAKE_PRIVATE_KEY,
            SNOWFLAKE_USER_NAME,
            NAME,
            SNOWFLAKE_ROLE_NAME,
            SNOWFLAKE_URL_NAME);
    List<String> paramsToRemove = new ArrayList<String>();

    for (String param : emptyParams) {
      paramsToRemove.add(param);
      this.invalidConfigRunner(paramsToRemove);
    }
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
        .hasMessageContaining(SNOWFLAKE_ROLE_NAME);
  }

  @Test
  public void shouldThrowExceptionWhenBothSSv2AndIcebergEnabled() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().withIcebergEnabled().build();

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("Ingestion to Iceberg table is currently unsupported")
        .hasMessageContaining(SNOWFLAKE_STREAMING_ICEBERG_ENABLED);
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
}
