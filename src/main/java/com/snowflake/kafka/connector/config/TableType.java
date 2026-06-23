package com.snowflake.kafka.connector.config;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/** Connector-wide target table type for auto-creation routing. */
public enum TableType {
  SNOWFLAKE,
  ICEBERG,
  NONE;

  /** Config string -> enum. Null/empty defaults to SNOWFLAKE (today's behavior). */
  public static TableType fromConfig(String value) {
    if (value == null || value.trim().isEmpty()) {
      return SNOWFLAKE;
    }
    String normalized = value.trim().toUpperCase(Locale.ROOT);
    try {
      return TableType.valueOf(normalized);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid snowflake.autocreate.table.type '"
              + value
              + "'. Allowed: "
              + Arrays.stream(values())
                  .map(t -> t.name().toLowerCase(Locale.ROOT))
                  .collect(Collectors.joining(", ")));
    }
  }

  public String configValue() {
    return name().toLowerCase(Locale.ROOT);
  }
}
