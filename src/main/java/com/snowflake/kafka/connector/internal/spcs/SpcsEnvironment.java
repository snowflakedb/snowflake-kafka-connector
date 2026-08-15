package com.snowflake.kafka.connector.internal.spcs;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    try {
      return new String(Files.readAllBytes(tokenPath), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read the SPCS session token at "
              + tokenPath
              + ". This file is provided by the SPCS runtime; ambient authentication is only"
              + " available to a connector running inside Snowpark Container Services.",
          e);
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
