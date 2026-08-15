/*
 * Copyright (c) 2025 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.spcs.SpcsEnvironment;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers ambient SPCS resolution at the {@link SinkTaskConfig} entry point.
 *
 * <p>This is one of the four places {@code SpcsEnvironment.resolve} is applied and, until this
 * test, the only one without direct coverage: it was exercised incidentally by other suites that
 * happen to call {@code SinkTaskConfig.from}. It matters on its own because it is the path taken by
 * task startup and by the connection factory, so a regression here would surface as a task-level
 * failure rather than a configuration error.
 */
public class SinkTaskConfigSpcsTest {

  private static final String HOST = "my-account.dep.us-west-2.aws.snowflakecomputing.com";

  @TempDir Path tempDir;

  @AfterEach
  public void reset() {
    SpcsEnvironment.resetForTests();
  }

  private void simulateSpcs() throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, HOST);
    // Literals rather than the package-private constants: this test lives in another package and
    // widening production visibility for a test's convenience is not worth it.
    env.put("SNOWFLAKE_DATABASE", "AMBIENT_DB");
    env.put("SNOWFLAKE_SCHEMA", "AMBIENT_SCHEMA");
    SpcsEnvironment.overrideForTests(env::get, token);
  }

  /** A credential-free raw map must become a fully populated SinkTaskConfig inside SPCS. */
  @Test
  public void shouldResolveAmbientValuesWhenBuildingSinkTaskConfig() throws IOException {
    simulateSpcs();

    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.NAME, "ambient_task_config");
    raw.put(KafkaConnectorConfigParams.TOPICS, "t1");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "SOME_ROLE");

    SinkTaskConfig config = SinkTaskConfig.from(raw, true);

    assertThat(config.getAuthenticator()).isEqualTo(AuthenticatorType.SPCS);
    assertThat(config.getSnowflakeUrl()).isEqualTo(HOST);
    assertThat(config.getSnowflakeDatabase()).isEqualTo("AMBIENT_DB");
    assertThat(config.getSnowflakeSchema()).isEqualTo("AMBIENT_SCHEMA");
    assertThat(config.getSnowflakeRole()).isEqualTo("SOME_ROLE");
    // The synthetic user exists only to satisfy validation; it is never sent to Snowflake.
    assertThat(config.getSnowflakeUser()).isEqualTo(SpcsEnvironment.AMBIENT_USER_PLACEHOLDER);
  }

  /**
   * Gate 3 in the design: {@code SnowflakeConnectionServiceFactory.setProperties} throws {@code
   * ERROR_0017} when the URL is blank. Ambient resolution is what keeps that gate satisfied without
   * relaxing it, so a resolved SPCS config must carry a non-blank URL by the time the connection
   * factory would read it.
   */
  @Test
  public void resolvedSpcsConfigSatisfiesTheUrlGate() throws IOException {
    simulateSpcs();

    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.NAME, "ambient_task_config");
    raw.put(KafkaConnectorConfigParams.TOPICS, "t1");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "SOME_ROLE");

    SinkTaskConfig config = SinkTaskConfig.from(raw, true);

    assertThat(config.getSnowflakeUrl()).isNotBlank();
  }

  /** Outside SPCS nothing may be invented, so the same raw map stays credential-free. */
  @Test
  public void shouldNotResolveAnythingOutsideSpcs() {
    SpcsEnvironment.overrideForTests(name -> null, tempDir.resolve("absent"));

    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.NAME, "ordinary");
    raw.put(KafkaConnectorConfigParams.TOPICS, "t1");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "SOME_ROLE");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, "explicit.snowflakecomputing.com");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME, "REAL_USER");

    SinkTaskConfig config = SinkTaskConfig.from(raw, true);

    assertThat(config.getAuthenticator()).isNotEqualTo(AuthenticatorType.SPCS);
    assertThat(config.getSnowflakeUser()).isEqualTo("REAL_USER");
    assertThat(config.getSnowflakeUrl()).isEqualTo("explicit.snowflakecomputing.com");
  }
}
