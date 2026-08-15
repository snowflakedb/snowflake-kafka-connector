package com.snowflake.kafka.connector.internal.spcs;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.AuthenticatorType;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Access to the credentials that Snowpark Container Services (SPCS) provides to every service
 * container, so a connector running inside SPCS needs no credential configuration.
 *
 * <p>Snowflake calls these <b>Snowflake-provided service user credentials</b>, or simply <i>service
 * credentials</i>. They are also widely referred to as <b>ambient authentication</b>, the
 * <i>ambient service identity</i>, or just <i>SPCS ambient</i>; those are informal names for the
 * same documented mechanism. See <a
 * href="https://docs.snowflake.com/en/developer-guide/snowpark-container-services/spcs-execute-sql">Snowpark
 * Container Services: SQL execution</a>.
 *
 * <p>When a service starts, Snowflake:
 *
 * <ul>
 *   <li>writes an OAuth token to {@value #DEFAULT_TOKEN_FILE}, which authenticates as the service
 *       user and is valid only inside that service;
 *   <li>sets {@code SNOWFLAKE_ACCOUNT} to the account identifier and {@code SNOWFLAKE_HOST} to the
 *       hostname to connect to. The token cannot be used without {@code SNOWFLAKE_HOST};
 *   <li>sets {@code SNOWFLAKE_DATABASE} and {@code SNOWFLAKE_SCHEMA} to the service's own database
 *       and schema. Note that no warehouse is provided.
 * </ul>
 *
 * <p>The resulting session runs as the service user, whose only roles are the service owner role
 * and PUBLIC, with the service owner role as its default.
 *
 * <p>The token is deliberately <b>never cached</b>. Snowflake rewrites the file every few minutes
 * and each token is valid for at most an hour, so a cached value would eventually be stale. Note
 * that, per the documentation, once a connection has been established successfully the token's
 * expiry no longer applies to that connection, exactly as for a session a user creates directly.
 */
public final class SpcsEnvironment {

  private static final KCLogger LOGGER = new KCLogger(SpcsEnvironment.class.getName());

  /** Bearer token file managed by the SPCS runtime. */
  public static final String DEFAULT_TOKEN_FILE = "/snowflake/session/token";

  public static final String ENV_HOST = "SNOWFLAKE_HOST";

  static final String ENV_DATABASE = "SNOWFLAKE_DATABASE";
  static final String ENV_SCHEMA = "SNOWFLAKE_SCHEMA";

  /**
   * Placeholder written to {@code snowflake.user.name} in ambient mode. The effective identity is
   * carried by the token rather than by this value, but several code paths require a non-blank
   * user.
   *
   * <p>Not the real service user name. Snowflake names the service user after the service itself,
   * or {@code SF$SERVICE$<unique-id>} for services created before the 8.35 release, which this
   * class has no reliable way to discover from inside the container.
   *
   * <p><b>This value must never be sent to Snowflake.</b> It exists only to satisfy the connector's
   * own requirement that a user name be configured. The ambient token already identifies the
   * service user, and a session that asserts a different user is rejected with "The user you were
   * trying to authenticate as differs from the user tied to the access token". Both {@code
   * InternalUtils.makeJdbcDriverProperties} and {@code StreamingClientProperties} therefore omit
   * the user entirely when the authenticator is {@code spcs}. Verified against a live SPCS service.
   */
  public static final String AMBIENT_USER_PLACEHOLDER = "spcs-service-user";

  /** Environment lookup. Overridable so tests can simulate an SPCS container. */
  private static Function<String, String> envReader = System::getenv;

  /** Token file location. Overridable so tests can point at a temporary file. */
  private static Path tokenPath = Paths.get(DEFAULT_TOKEN_FILE);

  private SpcsEnvironment() {}

  /**
   * @return true when running inside SPCS with an ambient identity available
   */
  public static boolean isInsideSpcs() {
    return env(ENV_HOST).isPresent() && Files.isReadable(tokenPath);
  }

  public static Optional<String> host() {
    return env(ENV_HOST);
  }

  public static Optional<String> database() {
    return env(ENV_DATABASE);
  }

  public static Optional<String> schema() {
    return env(ENV_SCHEMA);
  }

  /**
   * Reads the current ambient bearer token, re-reading the file on every call by design; see the
   * class comment.
   *
   * @throws IllegalStateException if the token file cannot be read
   */
  public static String readToken() {
    String token;
    try {
      token = new String(Files.readAllBytes(tokenPath), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read the SPCS session token at "
              + tokenPath
              + ". This file is provided by the SPCS runtime; ambient authentication is only"
              + " available to a connector running inside Snowpark Container Services.",
          e);
    }
    if (token.isEmpty()) {
      // Fail here rather than sending an empty bearer token. Snowflake rejects it with
      //   390303 Invalid OAuth access token.
      // which is accurate but says nothing about which file was empty or why, and arrives
      // only after a network round trip. An empty token means the platform did not supply a
      // credential, which is a local, inspectable condition.
      throw new IllegalStateException(
          "The SPCS session token at "
              + tokenPath
              + " is empty. Check that the service specification sets"
              + " capabilities.securityContext.enableCustomCredentials: true"
              + " — without it the server returns 390303 Invalid OAuth access token.");
    }
    return token;
  }

  /**
   * Fills in the values Snowflake provides to a service container, so the rest of the connector
   * sees an ordinary, complete configuration.
   *
   * <p>This is the single entry point for resolving the Snowflake-provided service user
   * credentials. It is a no-op outside SPCS and never overwrites a value the user supplied, so
   * explicit configuration always wins. It is also idempotent, so applying it at more than one
   * layer is safe.
   *
   * @param raw raw connector configuration
   * @return {@code raw} unchanged when outside SPCS, otherwise a copy with ambient values added
   */
  public static Map<String, String> resolve(Map<String, String> raw) {
    if (raw == null || !isInsideSpcs()) {
      return raw;
    }

    String configuredAuthenticator = raw.get(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR);

    if (isBlank(configuredAuthenticator)) {
      // No authenticator was configured. That does NOT mean "no credential": an absent value
      // makes AuthenticatorType.fromConfig return SNOWFLAKE_JWT, so a deployment that supplies
      // only a private key is a key-pair deployment that never had to name its authenticator.
      // Adopting ambient authentication here would silently ignore that credential and connect
      // as the SPCS service user instead, which is a different identity with different grants.
      // So only adopt when there is no credential at all to ignore.
      if (hasConfiguredCredential(raw)) {
        LOGGER.info(
            "Running inside Snowpark Container Services, but a credential is configured, so the"
                + " existing authentication method is kept. Set '{}' to '{}' to use ambient SPCS"
                + " authentication instead.",
            KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
            AuthenticatorType.SPCS.toConfigValue());
        return raw;
      }
      LOGGER.info(
          "Running inside Snowpark Container Services and '{}' was not configured; using ambient"
              + " SPCS authentication.",
          KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR);
    } else if (!AuthenticatorType.SPCS
        .toConfigValue()
        .equalsIgnoreCase(configuredAuthenticator.trim())) {
      // An authenticator was chosen explicitly. Change nothing.
      // The value is trimmed before comparison so this agrees with
      // AuthenticatorType.fromConfig, which also trims. Without the trim, a configuration
      // written as "snowflake.authenticator = spcs " parses as SPCS later but is treated
      // here as some other authenticator, so nothing is resolved and the connector then
      // fails with "snowflake.url.name must be provided" for no visible reason.
      return raw;
    }

    Map<String, String> resolved = new HashMap<>(raw);
    resolved.put(
        KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR, AuthenticatorType.SPCS.toConfigValue());
    putIfBlank(resolved, KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, host());
    putIfBlank(resolved, KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME, database());
    putIfBlank(resolved, KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME, schema());
    putIfBlank(
        resolved,
        KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
        Optional.of(AMBIENT_USER_PLACEHOLDER));

    if (!isBlank(resolved.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY))) {
      LOGGER.warn(
          "'{}' is set but will be ignored: ambient SPCS authentication uses the token supplied by"
              + " the SPCS runtime.",
          KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    }

    return resolved;
  }

  /**
   * True when the configuration already carries a credential, so switching authentication method
   * would discard it. Covers the key-pair and OAuth credential fields; the non-secret OAuth
   * settings (token endpoint, scope) are not credentials and are deliberately not consulted.
   */
  private static boolean hasConfiguredCredential(Map<String, String> config) {
    return !isBlank(config.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY))
        || !isBlank(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID))
        || !isBlank(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET))
        || !isBlank(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_REFRESH_TOKEN));
  }

  private static void putIfBlank(Map<String, String> config, String key, Optional<String> value) {
    if (isBlank(config.get(key))) {
      value.ifPresent(v -> config.put(key, v));
    }
  }

  private static Optional<String> env(String name) {
    return Optional.ofNullable(envReader.apply(name)).map(String::trim).filter(v -> !v.isEmpty());
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  @VisibleForTesting
  public static void overrideForTests(Function<String, String> env, Path token) {
    envReader = env;
    tokenPath = token;
  }

  @VisibleForTesting
  public static void resetForTests() {
    envReader = System::getenv;
    tokenPath = Paths.get(DEFAULT_TOKEN_FILE);
  }
}
