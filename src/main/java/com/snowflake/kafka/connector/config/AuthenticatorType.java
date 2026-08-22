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
  OAUTH,

  /**
   * Ambient Snowpark Container Services (SPCS) authentication, using the Snowflake-provided service
   * user credentials. The bearer token is supplied by the SPCS runtime, so no credential is
   * configured. Only valid for a connector running inside SPCS.
   *
   * @see <a
   *     href="https://docs.snowflake.com/en/developer-guide/snowpark-container-services/spcs-execute-sql">Snowpark
   *     Container Services: SQL execution</a>
   */
  SPCS;

  /**
   * Whether the credential itself identifies the Snowflake user, so the connector must not assert
   * one.
   *
   * <p>This is a property of the authentication method, not of a particular enum constant, and the
   * call sites are written against it rather than against {@code == SPCS} so that a future ambient
   * authenticator (for example workload identity federation) gets the correct behavior by declaring
   * it here, instead of by finding every place that compares the enum.
   *
   * <p>When this is true, two things follow, both verified against a live SPCS service:
   *
   * <ul>
   *   <li>The {@code user} property must be omitted from the credentials handed to the JDBC driver
   *       and to the streaming SDK. Snowflake rejects a session whose asserted user differs from
   *       the subject of the supplied token: {@code 390309 The user you were trying to authenticate
   *       as differs from the user tied to the access token}.
   *   <li>{@code SnowflakeErrors.ERROR_0016}, which requires a user to be present in the JDBC
   *       properties, must not fire, because the user is deliberately absent.
   * </ul>
   *
   * <p>A synthetic user is still written into the <i>configuration</i> so the existing validator
   * checks pass unchanged; it simply never reaches Snowflake. See {@code
   * SpcsEnvironment.AMBIENT_USER_PLACEHOLDER}.
   */
  public boolean suppliesAmbientIdentity() {
    return this == SPCS;
  }

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
