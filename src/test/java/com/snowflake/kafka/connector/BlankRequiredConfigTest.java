package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.spcs.SpcsEnvironment;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The three required connection properties reported errors that claimed to check for emptiness
 * while actually testing only for key presence:
 *
 * <pre>
 *   if (!config.containsKey(SNOWFLAKE_URL_NAME)) {
 *     invalidConfigParams.put(SNOWFLAKE_URL_NAME, "snowflake.url.name cannot be empty.");
 *   }
 * </pre>
 *
 * <p>So a property present with an empty value passed validation. That was not cosmetic. Measured
 * on the unfixed code, {@code snowflake.url.name=} was accepted by the validator and then made
 * {@link StreamingClientProperties#from} return a property set with <b>zero entries</b>: no
 * authorization type, no account, host, user or role, and no credential. The connector started and
 * failed later inside the SDK with an error naming something unrelated. One missing value silently
 * suppressed six.
 */
public class BlankRequiredConfigTest {

  private final ConnectorConfigValidator validator =
      new DefaultConnectorConfigValidator(new DefaultStreamingConfigValidator());

  /**
   * The point of the change: a present-but-empty value is now rejected, with the message the code
   * has always printed. Whitespace counts as empty, because a properties file makes that easy to
   * produce by accident and it is no more usable than a truly empty value.
   */
  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\t"})
  void shouldRejectABlankUrl(String blank) {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.put(SNOWFLAKE_URL_NAME, blank);

    assertThatThrownBy(() -> validator.validateConfig(config))
        .hasMessageContaining(SNOWFLAKE_URL_NAME);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\t"})
  void shouldRejectABlankUser(String blank) {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.put(SNOWFLAKE_USER_NAME, blank);

    assertThatThrownBy(() -> validator.validateConfig(config))
        .hasMessageContaining(SNOWFLAKE_USER_NAME);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\t"})
  void shouldRejectABlankRole(String blank) {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.put(SNOWFLAKE_ROLE_NAME, blank);

    assertThatThrownBy(() -> validator.validateConfig(config))
        .hasMessageContaining(SNOWFLAKE_ROLE_NAME);
  }

  /** An absent property must still be rejected, which is what the old check already did. */
  @Test
  void shouldStillRejectAnAbsentUrl() {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    config.remove(SNOWFLAKE_URL_NAME);

    assertThatThrownBy(() -> validator.validateConfig(config))
        .hasMessageContaining(SNOWFLAKE_URL_NAME);
  }

  /** A properly populated configuration must still pass, so the check is not simply stricter. */
  @Test
  void shouldAcceptAPopulatedConfiguration() {
    Map<String, String> config = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();

    validator.validateConfig(config);
  }

  /**
   * A blank URL now throws {@link
   * com.snowflake.kafka.connector.internal.SnowflakeErrors#ERROR_0017} rather than silently
   * returning an empty property set. A validated configuration cannot reach this branch; the tests
   * above enforce that. This test pins the behaviour for the case where a caller builds a
   * SinkTaskConfig without validating it first.
   */
  @Test
  void shouldThrowOnBlankUrl() {
    Map<String, String> raw = SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    raw.put(SNOWFLAKE_URL_NAME, "");
    raw.put("task_id", "0");
    SinkTaskConfig config = SinkTaskConfig.builderFrom(raw).build();

    assertThatThrownBy(() -> StreamingClientProperties.from(config))
        .as("blank URL is rejected with ERROR_0017 rather than silently returning empty properties")
        .isInstanceOf(com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0017");
  }

  @TempDir Path tempDir;

  @AfterEach
  void resetSpcs() {
    SpcsEnvironment.resetForTests();
  }

  /** Simulates a container running inside SPCS, with a token file and the runtime env vars. */
  private void simulateSpcs() throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, "myaccount.us-east-1.snowflakecomputing.com");
    env.put(SpcsEnvironment.ENV_ACCOUNT, "myaccount");
    env.put(SpcsEnvironment.ENV_DATABASE, "AMBIENT_DB");
    env.put(SpcsEnvironment.ENV_SCHEMA, "AMBIENT_SCHEMA");
    SpcsEnvironment.overrideForTests(env::get, token);
  }

  /** The minimal SPCS configuration from the documentation, which sets no role. */
  private static Map<String, String> documentedMinimalSpcsConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("name", "spcs_sink");
    config.put("connector.class", "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector");
    config.put("topics", "orders");
    config.put("snowflake.authenticator", "spcs");
    config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
    config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    config.put("value.converter.schemas.enable", "false");
    config.put("snowflake.streaming.validate.compatibility.with.classic", "false");
    return config;
  }

  /**
   * SNOW-4392681: under SPCS the session runs as the service user with the service owner's role, so
   * the documented configuration without {@code snowflake.role.name} must validate.
   */
  @Test
  void shouldAcceptTheDocumentedSpcsConfigWithoutARole() throws IOException {
    simulateSpcs();
    Map<String, String> config = documentedMinimalSpcsConfig();

    assertThatCode(() -> validator.validateConfig(config)).doesNotThrowAnyException();
  }

  @Test
  void shouldStillAcceptAnExplicitRoleUnderSpcs() throws IOException {
    simulateSpcs();
    Map<String, String> config = documentedMinimalSpcsConfig();
    config.put(SNOWFLAKE_ROLE_NAME, "SOME_ROLE");

    assertThatCode(() -> validator.validateConfig(config)).doesNotThrowAnyException();
  }

  /** Regression guard: the SPCS exemption must not relax key-pair authentication. */
  @Test
  void shouldStillRejectAnAbsentRoleForKeyPairAuthentication() {
    SpcsEnvironment.overrideForTests(name -> null, tempDir.resolve("absent"));
    Map<String, String> config =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().withoutRole().build();

    assertThatThrownBy(() -> validator.validateConfig(config))
        .hasMessageContaining(SNOWFLAKE_ROLE_NAME);
  }
}
