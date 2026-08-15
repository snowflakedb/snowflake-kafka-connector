package com.snowflake.kafka.connector.internal.spcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SpcsEnvironmentTest {

  private static final String HOST = "myaccount.us-east-1.snowflakecomputing.com";

  @TempDir Path tempDir;

  @AfterEach
  void reset() {
    SpcsEnvironment.resetForTests();
  }

  /** Simulates a container running inside SPCS, with a token file and the runtime env vars. */
  private Path simulateSpcs(String tokenValue) throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, tokenValue.getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, HOST);
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
}
