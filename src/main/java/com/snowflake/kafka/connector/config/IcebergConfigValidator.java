package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE_STREAMING;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.internal.parameters.InternalBufferParameters;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingConfigValidator;
import java.util.HashMap;
import java.util.Map;

/** Validates dependencies between parameters in Iceberg mode. */
public class IcebergConfigValidator implements StreamingConfigValidator {
  private static final String INCOMPATIBLE_INGESTION_METHOD =
      "Ingestion to Iceberg table is supported only for Snowpipe Streaming";

  private static final String DOUBLE_BUFFER_NOT_SUPPORTED =
      "Ingestion to Iceberg table is supported only with "
          + SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER
          + " enabled.";

  @Override
  public ImmutableMap<String, String> validate(Map<String, String> inputConfig) {
    boolean isIcebergEnabled = Boolean.parseBoolean(inputConfig.get(ICEBERG_ENABLED));

    if (!isIcebergEnabled) {
      return ImmutableMap.of();
    }

    Map<String, String> validationErrors = new HashMap<>();

    IngestionMethodConfig ingestionMethod =
        IngestionMethodConfig.valueOf(inputConfig.get(INGESTION_METHOD_OPT).toUpperCase());

    if (ingestionMethod != SNOWPIPE_STREAMING) {
      validationErrors.put(INGESTION_METHOD_OPT, INCOMPATIBLE_INGESTION_METHOD);
    }

    if (!InternalBufferParameters.isSingleBufferEnabled(inputConfig)) {
      validationErrors.put(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER, DOUBLE_BUFFER_NOT_SUPPORTED);
    }

    return ImmutableMap.copyOf(validationErrors);
  }
}
