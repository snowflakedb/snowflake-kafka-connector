package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/** Authentication method for Snowflake connections. */
public enum AuthenticatorType {
  /** Key-pair (JWT) authentication. This is the default. */
  SNOWFLAKE_JWT,

  /** External OAuth authentication. */
  OAUTH;

  /** The config string value, matching the v3 connector convention (lowercase with underscores). */
  public String toConfigValue() {
    return name().toLowerCase(Locale.ROOT);
  }

  /**
   * Parses a config string into an authenticator type (case-insensitive). Returns {@link
   * #SNOWFLAKE_JWT} for null or empty input.
   *
   * @throws IllegalArgumentException for unrecognized values
   */
  public static AuthenticatorType fromConfig(String value) {
    if (value == null || value.trim().isEmpty()) {
      return SNOWFLAKE_JWT;
    }
    String normalized = value.trim().toUpperCase(Locale.ROOT);
    try {
      return valueOf(normalized);
    } catch (IllegalArgumentException e) {
      String validValues =
          Arrays.stream(values())
              .map(AuthenticatorType::toConfigValue)
              .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          "Invalid value '"
              + value.trim()
              + "' for config '"
              + SNOWFLAKE_AUTHENTICATOR
              + "'. Valid values are: "
              + validValues,
          e);
    }
  }
}
