package com.snowflake.kafka.connector.internal.streaming.v2.migration;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_DEFAULT;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Controls whether the connector reads committed offsets from SSv1 channels during migration from
 * KC v3 to KC v4. Only consulted when the SSv2 channel has no committed offset yet.
 */
public enum Ssv1MigrationMode {
  /** Do not query SSv1 at all (default, current behavior). */
  SKIP,

  /**
   * If SSv2 has no committed offset, query SSv1 and use its offset as the starting point. If the
   * SSv1 channel is not found, fall through to the consumer group offset.
   */
  BEST_EFFORT,

  /**
   * If SSv2 has no committed offset, query SSv1 and use its offset as the starting point. If the
   * SSv1 channel is not found, fail the channel open so the operator can investigate.
   */
  STRICT;

  /**
   * Parses a config string into a migration mode (case-insensitive). Falls back to {@link
   * com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams#SNOWFLAKE_SSV1_OFFSET_MIGRATION_DEFAULT
   * SNOWFLAKE_SSV1_OFFSET_MIGRATION_DEFAULT} for null or empty input. Throws {@link
   * IllegalArgumentException} for unrecognized values, including the config key and valid options.
   */
  public static Ssv1MigrationMode fromConfig(String value) {
    if (value == null || value.trim().isEmpty()) {
      value = SNOWFLAKE_SSV1_OFFSET_MIGRATION_DEFAULT;
    }
    String normalized = value.trim().toUpperCase(Locale.ROOT);
    try {
      return valueOf(normalized);
    } catch (IllegalArgumentException e) {
      String validValues =
          Arrays.stream(values())
              .map(v -> v.name().toLowerCase(Locale.ROOT))
              .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "Invalid value '"
              + value.trim()
              + "' for config '"
              + SNOWFLAKE_SSV1_OFFSET_MIGRATION
              + "'. Valid values are: "
              + validValues,
          e);
    }
  }
}
