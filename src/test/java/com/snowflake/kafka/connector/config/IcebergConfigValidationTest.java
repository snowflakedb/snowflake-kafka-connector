package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingConfigValidator;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergConfigValidationTest {

  private static final StreamingConfigValidator validator = new IcebergConfigValidator();

  @ParameterizedTest
  @MethodSource("validConfigs")
  public void shouldValidateCorrectConfig(Map<String, String> config) {
    // when
    ImmutableMap<String, String> invalidParameters = validator.validate(config);

    // then
    Assertions.assertTrue(invalidParameters.isEmpty());
  }

  @ParameterizedTest
  @MethodSource("invalidConfigs")
  public void shouldReturnErrorOnInvalidConfig(Map<String, String> config, String errorKey) {
    // when
    ImmutableMap<String, String> invalidParameters = validator.validate(config);

    // then
    Assertions.assertTrue(invalidParameters.containsKey(errorKey));
  }

  public static Stream<Arguments> validConfigs() {
    return Stream.of(
        Arguments.of(SnowflakeSinkConnectorConfigBuilder.snowpipeConfig().build()),
        Arguments.of(SnowflakeSinkConnectorConfigBuilder.icebergConfig().build()),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.icebergConfig()
                .withSchematizationEnabled(false)
                .build()));
  }

  public static Stream<Arguments> invalidConfigs() {
    return Stream.of(
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.icebergConfig()
                .withIngestionMethod(IngestionMethodConfig.SNOWPIPE)
                .build(),
            INGESTION_METHOD_OPT),
        Arguments.of(
            SnowflakeSinkConnectorConfigBuilder.icebergConfig()
                .withSingleBufferEnabled(false)
                .build(),
            SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER));
  }
}
