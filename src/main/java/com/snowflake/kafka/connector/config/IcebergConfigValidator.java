package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE_STREAMING;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingConfigValidator;
import java.util.HashMap;
import java.util.Map;

/** Validates dependencies between parameters in Iceberg mode. */
public class IcebergConfigValidator implements StreamingConfigValidator {

  private static final String NO_SCHEMATIZATION_ERROR_MESSAGE =
      "Ingestion to Iceberg table requires " + ENABLE_SCHEMATIZATION_CONFIG + " set to true";
  private static final String INCOMPATIBLE_INGESTION_METHOD =
      "Ingestion to Iceberg table is supported only for Snowpipe Streaming";

  @Override
  public ImmutableMap<String, String> validate(Map<String, String> inputConfig) {
    boolean isIcebergEnabled = Boolean.parseBoolean(inputConfig.get(ICEBERG_ENABLED));

    if (!isIcebergEnabled) {
      return ImmutableMap.of();
    }

    Map<String, String> validationErrors = new HashMap<>();

    boolean isSchematizationEnabled =
        Boolean.parseBoolean(
            inputConfig.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG));
    IngestionMethodConfig ingestionMethod =
        IngestionMethodConfig.valueOf(inputConfig.get(INGESTION_METHOD_OPT).toUpperCase());

    if (!isSchematizationEnabled) {
      validationErrors.put(ENABLE_SCHEMATIZATION_CONFIG, NO_SCHEMATIZATION_ERROR_MESSAGE);
    }
    if (ingestionMethod != SNOWPIPE_STREAMING) {
      validationErrors.put(INGESTION_METHOD_OPT, INCOMPATIBLE_INGESTION_METHOD);
    }

    return ImmutableMap.copyOf(validationErrors);
  }
}
