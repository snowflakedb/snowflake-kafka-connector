package com.snowflake.kafka.connector.internal.spcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.AuthenticatorType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SpcsEnvironmentTest {

  private static final String HOST = "myaccount.us-east-1.snowflakecomputing.com";

  @TempDir Path tempDir;

  @AfterEach
  void reset() {
    SpcsEnvironment.resetForTests();
  }

  private static final String ACCOUNT = "myaccount";

  /** Simulates a container running inside SPCS, with a token file and the runtime env vars. */
  private Path simulateSpcs(String tokenValue) throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, tokenValue.getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, HOST);
    env.put(SpcsEnvironment.ENV_ACCOUNT, ACCOUNT);
    env.put(SpcsEnvironment.ENV_DATABASE, "AMBIENT_DB");
    env.put(SpcsEnvironment.ENV_SCHEMA, "AMBIENT_SCHEMA");
    SpcsEnvironment.overrideForTests(env::get, token);
    return token;
  }

  private void simulateOutsideSpcs() {
    SpcsEnvironment.overrideForTests(name -> null, tempDir.resolve("does-not-exist"));
  }

  @Test
  void shouldDetectSpcsWhenHostAndTokenPresent() throws IOException {
    simulateSpcs("tok");
    assertThat(SpcsEnvironment.isInsideSpcs()).isTrue();
  }

  @Test
  void shouldNotDetectSpcsWhenTokenFileMissing() {
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, HOST);
    SpcsEnvironment.overrideForTests(env::get, tempDir.resolve("absent"));

    assertThat(SpcsEnvironment.isInsideSpcs()).isFalse();
  }

  @Test
  void shouldNotDetectSpcsWhenHostEnvMissing() throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "tok".getBytes(StandardCharsets.UTF_8));
    SpcsEnvironment.overrideForTests(name -> null, token);

    assertThat(SpcsEnvironment.isInsideSpcs()).isFalse();
  }

  /**
   * The token must never be cached. SPCS rewrites the file every few minutes and each token is
   * valid for at most an hour, so a cached value would expire silently.
   */
  @Test
  void shouldRereadTokenOnEveryCallRatherThanCachingIt() throws IOException {
    Path token = simulateSpcs("first-token");
    assertThat(SpcsEnvironment.readToken()).isEqualTo("first-token");

    Files.write(token, "rotated-token".getBytes(StandardCharsets.UTF_8));

    assertThat(SpcsEnvironment.readToken()).isEqualTo("rotated-token");
  }

  @Test
  void shouldTrimWhitespaceFromToken() throws IOException {
    simulateSpcs("  padded-token\n");
    assertThat(SpcsEnvironment.readToken()).isEqualTo("padded-token");
  }

  @Test
  void shouldFailWithActionableMessageWhenTokenUnreadable() {
    simulateOutsideSpcs();
    assertThatThrownBy(SpcsEnvironment::readToken)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Snowpark Container Services");
  }

  @Test
  void shouldBeNoOpOutsideSpcs() {
    simulateOutsideSpcs();
    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.NAME, "testConnector");

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved).isEqualTo(raw);
    assertThat(resolved).doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR);
  }

  @Test
  void shouldTolerateNullConfig() {
    simulateOutsideSpcs();
    assertThat(SpcsEnvironment.resolve(null)).isNull();
  }

  @Test
  void shouldFillInAmbientValuesInsideSpcs() throws IOException {
    simulateSpcs("tok");

    Map<String, String> resolved = SpcsEnvironment.resolve(new HashMap<>());

    assertThat(resolved)
        .containsEntry(
            KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
            AuthenticatorType.SPCS.toConfigValue())
        .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, HOST)
        .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME, "AMBIENT_DB")
        .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME, "AMBIENT_SCHEMA")
        .containsEntry(
            KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
            SpcsEnvironment.AMBIENT_USER_PLACEHOLDER);
  }

  @Test
  void shouldNotOverwriteUserSuppliedValues() throws IOException {
    simulateSpcs("tok");
    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME, "MY_DB");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME, "MY_SCHEMA");

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved)
        .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME, "MY_DB")
        .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME, "MY_SCHEMA");
  }

  /** An explicitly configured authenticator must always win over ambient detection. */
  @Test
  void shouldLeaveConfigAloneWhenAnotherAuthenticatorIsExplicit() throws IOException {
    simulateSpcs("tok");
    Map<String, String> raw = new HashMap<>();
    raw.put(
        KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
        AuthenticatorType.SNOWFLAKE_JWT.toConfigValue());

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved)
        .containsEntry(
            KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
            AuthenticatorType.SNOWFLAKE_JWT.toConfigValue())
        .doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME)
        .doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME);
  }

  /**
   * The regression this guards against: an absent {@code snowflake.authenticator} does NOT mean "no
   * credential". {@link AuthenticatorType#fromConfig} returns {@code SNOWFLAKE_JWT} for an absent
   * value, so a deployment that supplies only a private key is a key-pair deployment that never had
   * to name its authenticator. If ambient authentication were adopted here, that key would be
   * ignored and the connector would silently connect as the SPCS service user, a different identity
   * with different grants. The configuration must be returned untouched.
   */
  @Test
  void shouldNotAdoptAmbientAuthWhenAKeyPairCredentialIsConfigured() throws IOException {
    simulateSpcs("tok");
    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, "a-private-key");

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved).isSameAs(raw);
    assertThat(resolved)
        .doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR)
        .doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME)
        .doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME)
        .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, "a-private-key");
  }

  /** The same protection must apply to an OAuth deployment that did not name its authenticator. */
  @ParameterizedTest
  @ValueSource(
      strings = {
        KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID,
        KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET,
        KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_REFRESH_TOKEN
      })
  void shouldNotAdoptAmbientAuthWhenAnOauthCredentialIsConfigured(String credentialKey)
      throws IOException {
    simulateSpcs("tok");
    Map<String, String> raw = new HashMap<>();
    raw.put(credentialKey, "a-value");

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved).isSameAs(raw);
    assertThat(resolved).doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR);
  }

  /**
   * The non-secret OAuth settings are not credentials, so they must not suppress adoption. Only a
   * client id, client secret, refresh token, or private key counts.
   */
  @Test
  void shouldStillAdoptAmbientAuthWhenOnlyNonSecretOauthSettingsArePresent() throws IOException {
    simulateSpcs("tok");
    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT, "https://example/token");

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved)
        .containsEntry(
            KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
            AuthenticatorType.SPCS.toConfigValue());
  }

  /**
   * An operator who asks for ambient authentication explicitly has made the choice knowingly, so a
   * leftover credential must not block it. It is ignored, with a warning.
   */
  @Test
  void shouldAdoptAmbientAuthDespiteACredentialWhenSpcsIsExplicit() throws IOException {
    simulateSpcs("tok");
    Map<String, String> raw = new HashMap<>();
    raw.put(
        KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR, AuthenticatorType.SPCS.toConfigValue());
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, "a-private-key");

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved)
        .containsEntry(
            KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
            AuthenticatorType.SPCS.toConfigValue())
        .containsEntry(
            KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
            SpcsEnvironment.AMBIENT_USER_PLACEHOLDER)
        .containsKey(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME);
  }

  /**
   * resolve() is applied at three layers (connector start, config validation, and config parsing),
   * so it must be idempotent: resolving an already-resolved map must change nothing.
   */
  @Test
  void shouldBeIdempotent() throws IOException {
    simulateSpcs("tok");

    Map<String, String> once = SpcsEnvironment.resolve(new HashMap<>());
    Map<String, String> twice = SpcsEnvironment.resolve(once);

    assertThat(twice).isEqualTo(once);
  }

  @Test
  void shouldStillFillValuesWhenSpcsAuthenticatorIsExplicit() throws IOException {
    simulateSpcs("tok");
    Map<String, String> raw = new HashMap<>();
    raw.put(
        KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR, AuthenticatorType.SPCS.toConfigValue());

    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved).containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, HOST);
  }

  /**
   * {@code resolve()} compares the configured authenticator with {@code equalsIgnoreCase}, so a
   * user who writes {@code SPCS} or {@code Spcs} must get ambient resolution just as one who writes
   * {@code spcs} does. Kafka Connect passes configuration through verbatim, so the casing is
   * whatever the operator typed.
   */
  @Test
  void shouldTreatExplicitSpcsAuthenticatorCaseInsensitively() throws IOException {
    for (String spelling : new String[] {"spcs", "SPCS", "Spcs", " spcs "}) {
      simulateSpcs("tok");
      Map<String, String> raw = new HashMap<>();
      raw.put(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR, spelling);

      Map<String, String> resolved = SpcsEnvironment.resolve(raw);

      assertThat(resolved)
          .as("authenticator spelled '%s' must still resolve ambient values", spelling)
          .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, HOST)
          .containsEntry(
              KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
              SpcsEnvironment.AMBIENT_USER_PLACEHOLDER);
      SpcsEnvironment.resetForTests();
    }
  }

  /**
   * The database and schema are supplied by the SPCS runtime, but nothing guarantees they are
   * present. When they are absent, resolution must leave them absent rather than invent them, so
   * that the existing validator reports a clear configuration error instead of the connector
   * failing later against the wrong schema.
   */
  @Test
  void shouldNotInventDatabaseOrSchemaWhenTheRuntimeDoesNotSupplyThem() throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "tok".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, HOST);
    // deliberately no SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA
    SpcsEnvironment.overrideForTests(env::get, token);

    Map<String, String> resolved = SpcsEnvironment.resolve(new HashMap<>());

    assertThat(resolved).containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, HOST);
    assertThat(resolved).doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME);
    assertThat(resolved).doesNotContainKey(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME);
  }

  /**
   * An empty token file fails fast, locally, rather than being sent to Snowflake.
   *
   * <p>Snowflake rejects an empty bearer token with {@code 390303 Invalid OAuth access token}
   * (measured). That is accurate but names neither the file nor the cause, and it costs a network
   * round trip. An empty token means the runtime supplied no credential, which is a local and
   * inspectable condition, so the message names the path, quotes the error that would otherwise be
   * returned, and points at the most common cause.
   */
  @Test
  void shouldFailFastOnAnEmptyTokenFileRatherThanSendingIt() throws IOException {
    simulateSpcs("");

    // Detection is unchanged: the file exists and is readable, so this still looks like SPCS.
    assertThat(SpcsEnvironment.isInsideSpcs()).isTrue();

    assertThatThrownBy(SpcsEnvironment::readToken)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("is empty")
        .hasMessageContaining("390303")
        .hasMessageContaining("enableCustomCredentials");
  }

  /** Whitespace-only is empty for this purpose: the token is trimmed before it is judged. */
  @Test
  void shouldTreatWhitespaceOnlyTokenAsEmpty() throws IOException {
    simulateSpcs("   \n\t  ");

    assertThatThrownBy(SpcsEnvironment::readToken)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("is empty");
  }

  /**
   * An empty token throws locally with an actionable message rather than sending the token to
   * Snowflake and waiting for its generic '390303 Invalid OAuth access token' rejection. The
   * message must name the cause ('enableCustomCredentials') so an operator can fix it without
   * reading source code. R19.
   */
  @Test
  void shouldRejectEmptyTokenLocallyWithActionableMessage() throws IOException {
    simulateSpcs("");

    assertThatThrownBy(SpcsEnvironment::readToken)
        .isInstanceOf(IllegalStateException.class)
        .as("message must name the Snowflake error so the operator knows what Snowflake would say")
        .hasMessageContaining("390303")
        .as("message must name the fix so the operator does not need to read the source")
        .hasMessageContaining("enableCustomCredentials");
  }

  /**
   * Security pin (R20): the bearer token must not leak into the resolved configuration map. The
   * token is read and used to build properties, but it must never appear as a config value -- doing
   * so would expose it to any code that logs or serializes the configuration map.
   */
  @Test
  void tokenMustNotAppearInResolvedConfigurationMap() throws IOException {
    String secret = "VERY-SECRET-TOKEN-" + System.nanoTime();
    simulateSpcs(secret);

    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.NAME, "testConnector");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "MY_ROLE");
    Map<String, String> resolved = SpcsEnvironment.resolve(raw);

    assertThat(resolved.values())
        .as("the bearer token value must not appear in any entry of the resolved config")
        .doesNotContain(secret);
  }

  /**
   * Security pin (R20): the bearer token must not appear in log output during resolution. A log
   * line that includes the token value would expose it to anyone with access to the log file.
   */
  @Test
  void tokenMustNotAppearInLogsDuringResolution() throws IOException {
    String secret = "VERY-SECRET-LOG-TOKEN-" + System.nanoTime();
    simulateSpcs(secret);

    Logger rootLogger = Logger.getRootLogger();
    CapturingAppender appender = new CapturingAppender();
    rootLogger.addAppender(appender);
    try {
      Map<String, String> raw = new HashMap<>();
      raw.put(KafkaConnectorConfigParams.NAME, "testConnector");
      raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "MY_ROLE");
      SpcsEnvironment.resolve(raw);
    } finally {
      rootLogger.removeAppender(appender);
    }

    List<String> messages = appender.getMessages();
    assertThat(messages)
        .as("the bearer token value must not appear in any log record emitted during resolution")
        .noneMatch(msg -> msg.contains(secret));
  }

  private static class CapturingAppender extends AppenderSkeleton {
    private final List<String> messages = new ArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      messages.add(event.getRenderedMessage());
    }

    @Override
    public void close() {}

    @Override
    public boolean requiresLayout() {
      return false;
    }

    List<String> getMessages() {
      return new ArrayList<>(messages);
    }
  }
}
