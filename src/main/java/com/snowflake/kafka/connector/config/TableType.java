package com.snowflake.kafka.connector.config;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/** Connector-wide target table type for auto-creation routing. */
public enum TableType {
  SNOWFLAKE,
  ICEBERG,
  NONE;

  /**
   * Config string -&gt; enum. Returns {@link Optional#empty()} when the value is {@code null} or
   * blank (absent/unset config). Throws {@link IllegalArgumentException} for a non-blank value that
   * is not a recognized type.
   *
   * <p>Callers should use {@code TableType.fromConfig(x).orElse(TableType.SNOWFLAKE)} to preserve
   * the original default behavior.
   */
  public static Optional<TableType> fromConfig(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Optional.empty();
    }
    String normalized = value.trim().toUpperCase(Locale.ROOT);
    try {
      return Optional.of(TableType.valueOf(normalized));
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
