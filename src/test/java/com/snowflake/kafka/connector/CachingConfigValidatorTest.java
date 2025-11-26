package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CachingConfigValidatorTest {

  private final ConnectorConfigValidator validator =
      new DefaultConnectorConfigValidator(new DefaultStreamingConfigValidator());

  private static Stream<Arguments> validCacheExpirations() {
    return Stream.of(Arguments.of("30000", "60000"), Arguments.of("3600000", "7200000"));
  }

  private static Stream<Arguments> validCacheEnabledDisabled() {
    return Stream.of(
        Arguments.of("true", "true"),
        Arguments.of("True", "True"),
        Arguments.of("TRUE", "TRUE"),
        Arguments.of("false", "false"),
        Arguments.of("False", "False"),
        Arguments.of("FALSE", "FALSE"));
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("validCacheExpirations")
  void test_valid_expirations(String tableExpireMs, String pipeExpireMs) {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.put(CACHE_TABLE_EXISTS_EXPIRE_MS, tableExpireMs);
    config.put(CACHE_PIPE_EXISTS_EXPIRE_MS, pipeExpireMs);

    assertDoesNotThrow(() -> validator.validateConfig(config));
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("validCacheEnabledDisabled")
  void test_valid_enabled_disabled(String tableExists, String pipeExists) {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.put(CACHE_TABLE_EXISTS, tableExists);
    config.put(CACHE_PIPE_EXISTS, pipeExists);

    assertDoesNotThrow(() -> validator.validateConfig(config));
  }

  private static Stream<Arguments> invalidConfigurationProvider() {
    return Stream.of(
        Arguments.of(CACHE_TABLE_EXISTS_EXPIRE_MS, "0", "Should reject zero table expiration"),
        Arguments.of(CACHE_TABLE_EXISTS_EXPIRE_MS, "-1", "Should reject negative table expiration"),
        Arguments.of(CACHE_PIPE_EXISTS_EXPIRE_MS, "0", "Should reject zero pipe expiration"),
        Arguments.of(
            CACHE_PIPE_EXISTS_EXPIRE_MS, "-5000", "Should reject negative pipe expiration"),
        Arguments.of(
            CACHE_TABLE_EXISTS_EXPIRE_MS, "invalid", "Should reject non-numeric table expiration"),
        Arguments.of(
            CACHE_PIPE_EXISTS_EXPIRE_MS,
            "not a number",
            "Should reject non-numeric pipe expiration"),
        Arguments.of(
            CACHE_TABLE_EXISTS, "blag blag", "Should reject invalid boolean for table exists"),
        Arguments.of(CACHE_TABLE_EXISTS, "ture", "Should reject typo in boolean for table exists"),
        Arguments.of(CACHE_TABLE_EXISTS, "1", "Should reject numeric boolean for table exists"),
        Arguments.of(
            CACHE_TABLE_EXISTS, "yes", "Should reject non-boolean string for table exists"),
        Arguments.of(CACHE_PIPE_EXISTS, "0", "Should reject numeric boolean for pipe exists"),
        Arguments.of(CACHE_PIPE_EXISTS, "no", "Should reject non-boolean string for pipe exists"));
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("invalidConfigurationProvider")
  void testInvalidCacheConfiguration(String configKey, String configValue, String description) {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.put(configKey, configValue);

    assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> validator.validateConfig(config),
        description);
  }
}
