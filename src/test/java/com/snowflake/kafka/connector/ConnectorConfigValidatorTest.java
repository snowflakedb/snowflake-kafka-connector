package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.Utils.HTTP_NON_PROXY_HOSTS;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_HOST;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_PORT;
import static com.snowflake.kafka.connector.Utils.HTTP_USE_PROXY;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfig;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertEquals;

import com.snowflake.kafka.connector.config.IcebergConfigValidator;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.config.TopicToTableModeExtractor;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
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

  private final ConnectorConfigValidator connectorConfigValidator =
      new DefaultConnectorConfigValidator(
          new DefaultStreamingConfigValidator(), new IcebergConfigValidator());

  // subset of valid community converters
  public static final List<Converter> COMMUNITY_CONVERTER_SUBSET =
      Arrays.asList(
          new org.apache.kafka.connect.storage.StringConverter(),
          new org.apache.kafka.connect.json.JsonConverter(),
          new io.confluent.connect.avro.AvroConverter());

  // custom snowflake converters, not currently allowed for streaming
  public static final List<Converter> CUSTOM_SNOWFLAKE_CONVERTERS =
      Arrays.asList(
          new com.snowflake.kafka.connector.records.SnowflakeJsonConverter(),
          new com.snowflake.kafka.connector.records.SnowflakeAvroConverterWithoutSchemaRegistry(),
          new com.snowflake.kafka.connector.records.SnowflakeAvroConverter());

  private static Stream<Arguments> customSnowflakeConverters() {
    return CUSTOM_SNOWFLAKE_CONVERTERS.stream().map(Arguments::of);
  }

  @Test
  public void testConfig() {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.snowpipeConfig().build();
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testConfig_ConvertedInvalidAppName() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.snowpipeConfig()
            .withName("testConfig.snowflake-connector")
            .build();

    Utils.convertAppName(config);

    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testEmptyFlushTime() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
  }

  @Test
  public void testFlushTimeSmall() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        (SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN - 1) + "");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
  }

  @Test
  public void testFlushTimeNotNumber() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "fdas");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
  }

  @ParameterizedTest
  @CsvSource({
    NAME,
    SNOWFLAKE_URL,
    SNOWFLAKE_USER,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_PRIVATE_KEY,
    BUFFER_SIZE_BYTES,
    BUFFER_COUNT_RECORDS
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
  public void testBufferSizeRange() {
    Map<String, String> config = getConfig();
    config.put(BUFFER_SIZE_BYTES, SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN - 1 + "");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(BUFFER_SIZE_BYTES);
  }

  @Test
  public void testBufferSizeValue() {
    Map<String, String> config = getConfig();
    config.put(BUFFER_SIZE_BYTES, "afdsa");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(BUFFER_SIZE_BYTES);
  }

  @Test
  public void testEmptyBufferCountNegative() {
    Map<String, String> config = getConfig();
    config.put(BUFFER_COUNT_RECORDS, "-1");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(BUFFER_COUNT_RECORDS);
  }

  @Test
  public void testBufferCountValue() {
    Map<String, String> config = getConfig();
    config.put(BUFFER_COUNT_RECORDS, "adssadsa");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(BUFFER_COUNT_RECORDS);
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
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE.toString());
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_valid_value_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_invalid_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(Utils.SF_ROLE);
  }

  @Test
  public void testIngestionTypeConfig_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT, "INVALID_VALUE");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT);
  }

  @Test
  public void testDetermineIngestionMethod_nullOrEmptyInput() {
    Map<String, String> config = getConfig();
    assertEquals(
        IngestionMethodConfig.SNOWPIPE, IngestionMethodConfig.determineIngestionMethod(config));

    assertEquals(
        IngestionMethodConfig.SNOWPIPE, IngestionMethodConfig.determineIngestionMethod(null));
  }

  @Test
  public void testDetermineIngestionMethod_invalidIngestionMethod() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT, "INVALID_VALUE");

    assertThatThrownBy(() -> IngestionMethodConfig.determineIngestionMethod(config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testDetermineIngestionLoadMethod_validIngestionMethod() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT, "SNOWPIPE_STREAMING");
    assertEquals(
        IngestionMethodConfig.SNOWPIPE_STREAMING,
        IngestionMethodConfig.determineIngestionMethod(config));
  }

  @Test
  public void testDetermineIngestionLoadMethod_validIngestionMethod_lowercase() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT, "snowpipe_stREAMING");
    assertEquals(
        IngestionMethodConfig.SNOWPIPE_STREAMING,
        IngestionMethodConfig.determineIngestionMethod(config));
  }

  /** These error tests are not going to enforce errors if they are not passed as configs. */
  @Test
  public void testErrorTolerance_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
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
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT);
  }

  @Test
  public void testErrorLog_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_LOG_ENABLE_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
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
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG);
  }

  // ---------- Streaming Buffer tests ---------- //
  @Test
  public void testStreamingEmptyFlushTime() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
  }

  @Test
  public void testStreamingFlushTimeSmall() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        (StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC - 1) + "");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
  }

  @Test
  public void testStreamingFlushTimeNotNumber() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "fdas");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
  }

  @Test
  public void testStreamingEmptyBufferSize() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    config.remove(BUFFER_SIZE_BYTES);
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES);
  }

  @Test
  public void testStreamingEmptyBufferCount() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    config.remove(BUFFER_COUNT_RECORDS);
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(BUFFER_COUNT_RECORDS);
  }

  @Test
  public void testStreamingBufferCountNegative() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    config.put(BUFFER_COUNT_RECORDS, "-1");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(BUFFER_COUNT_RECORDS);
  }

  @Test
  public void testStreamingBufferCountValue() {
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    config.put(BUFFER_COUNT_RECORDS, "adssadsa");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(BUFFER_COUNT_RECORDS);
  }

  @Test
  public void testValidKeyAndValueConvertersForStreamingSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
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
  @MethodSource("customSnowflakeConverters")
  public void testInvalidKeyConvertersForStreamingSnowpipe(Converter converter) {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");

    config.put(
        SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD, converter.getClass().getName());
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD);
  }

  @ParameterizedTest
  @MethodSource("customSnowflakeConverters")
  public void testInvalidValueConvertersForStreamingSnowpipe(Converter converter) {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");

    config.put(
        SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD, converter.getClass().getName());

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD);
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
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
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
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(prop, value);

    // WHEN/THEN
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(prop);
  }

  @ParameterizedTest
  @CsvSource({
    SNOWPIPE_STREAMING_MAX_CLIENT_LAG + ", 10",
    SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES + ", 10"
  })
  public void shouldThrowExceptionForStreamingClientPropsWhenSnowpipeStreamingNotEnabled(
      String prop, String value) {
    // GIVEN
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(prop, value);

    // WHEN/THEN
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(prop);
  }

  @Test
  public void testInvalidSchematizationForSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE.toString());
    config.put(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
  }

  @Test
  public void testValidSchematizationForStreamingSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testSchematizationWithUnsupportedConverter() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, "true");
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
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, "false");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testEnableOptimizeStreamingClientConfig() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "true");

    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testInvalidEnableOptimizeStreamingClientConfig() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE.toString());
    config.put(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "true");

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG);
  }

  @Test
  public void testEnableStreamingChannelMigrationConfig() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG, "true");

    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testEnableStreamingChannelMigrationConfig_invalidWithSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE.toString());
    config.put(SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG, "true");

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(
            SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG);
  }

  @Test
  public void
      testEnableStreamingChannelMigrationConfig_invalidBooleanValue_WithSnowpipeStreaming() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG, "INVALID");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(
            SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG);
  }

  @Test
  public void testEnableStreamingChannelOffsetVerificationConfig() {
    // given
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withRole("ACCOUNTADMIN")
            .withChannelOffsetTokenVerificationFunctionEnabled(true)
            .build();

    // when, then
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testEnableStreamingChannelOffsetVerificationConfig_invalidWithSnowpipe() {
    // given
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.snowpipeConfig()
            .withChannelOffsetTokenVerificationFunctionEnabled(true)
            .build();

    // when, then
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(
            SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG);
  }

  @Test
  public void
      testEnableStreamingChannelOffsetVerificationConfig_invalidBooleanValue_WithSnowpipeStreaming() {
    // given
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.put(
        SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG,
        "INVALID");

    // when, then
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(
            SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG);
  }

  @Test
  public void testStreamingProviderOverrideConfig_invalidWithSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE.toString());
    config.put(
        SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "a:b,c:d");

    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(
            SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);
  }

  @Test
  public void testStreamingProviderOverrideConfig_validWithSnowpipeStreaming() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
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
        .hasMessageContaining(BUFFER_COUNT_RECORDS)
        .hasMessageContaining(SNOWFLAKE_PRIVATE_KEY)
        .hasMessageContaining(SNOWFLAKE_USER)
        .hasMessageContaining(NAME)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC)
        .hasMessageContaining(SNOWFLAKE_URL)
        .hasMessageContaining(BUFFER_SIZE_BYTES);
  }

  // removes each of the following params iteratively to test if the log/exception has all the
  // expected removed params
  @Test
  public void testMultipleInvalidConfigs() {
    List<String> emptyParams =
        Arrays.asList(
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_SCHEMA,
            BUFFER_COUNT_RECORDS,
            SNOWFLAKE_PRIVATE_KEY,
            SNOWFLAKE_USER,
            NAME,
            SNOWFLAKE_URL,
            BUFFER_SIZE_BYTES);
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
        SnowflakeSinkConnectorConfigBuilder.snowpipeConfig()
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
  public void testENABLE_REPROCESS_FILES_CLEANUP_valid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP, "true");
    connectorConfigValidator.validateConfig(config);
    config.put(SnowflakeSinkConnectorConfig.SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP, "False");
    connectorConfigValidator.validateConfig(config);
  }

  @Test
  public void testENABLE_REPROCESS_FILES_CLEANUP_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP, "INVALID");
    assertThatThrownBy(() -> connectorConfigValidator.validateConfig(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SnowflakeSinkConnectorConfig.SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP);
  }
}
