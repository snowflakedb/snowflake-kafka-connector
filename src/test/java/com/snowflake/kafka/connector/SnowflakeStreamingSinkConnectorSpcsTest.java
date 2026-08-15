/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.AuthenticatorType;
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
 * Guards the ambient-credential resolution at the connector's entry point.
 *
 * <p>This exists because of a defect that was actually introduced and fixed during development: the
 * configuration was resolved for validation but not for the connector's own {@code config} field,
 * so the connection it built and the configs it handed to tasks disagreed with what had been
 * validated. Nothing caught it, because {@code SnowflakeStreamingSinkConnector} is otherwise
 * touched only by integration tests, and those do not run inside SPCS.
 *
 * <p>{@code start()} is allowed to fail here. It builds a real Snowflake connection a few
 * statements after assigning {@code config}, which cannot succeed in a unit test. The assignment is
 * what is under test, and it happens first.
 */
public class SnowflakeStreamingSinkConnectorSpcsTest {

  @AfterEach
  public void resetSpcsEnvironment() {
    SpcsEnvironment.resetForTests();
  }

  @Test
  public void startResolvesAmbientCredentialsIntoTheConnectorsOwnConfig(@TempDir Path tempDir)
      throws IOException {
    // GIVEN a container that looks like SPCS, with the host shape measured inside a real service
    Path token = tempDir.resolve("token");
    Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, "myaccount.dep.us-west-2.aws.snowflakecomputing.com");
    env.put("SNOWFLAKE_DATABASE", "AMBIENT_DB");
    env.put("SNOWFLAKE_SCHEMA", "AMBIENT_SCHEMA");
    SpcsEnvironment.overrideForTests(env::get, token);

    // AND a configuration carrying no credential and no connection coordinates at all
    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.NAME, "ambient_connector");
    raw.put(KafkaConnectorConfigParams.TOPICS, "t1");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "SOME_ROLE");

    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector();

    // WHEN start() runs. It will not complete: a real connection cannot be built here. The
    // assignment under test happens before that, so the outcome of start() is irrelevant.
    try {
      connector.start(raw);
    } catch (Throwable expected) {
      // ignored on purpose, see the class comment
    }

    // THEN the connector's own config carries the ambient values, not just the validated copy
    Map<String, String> effective = connector.effectiveConfigForTests();
    assertThat(effective)
        .as("start() must resolve ambient credentials into the config it keeps and passes on")
        .isNotNull();
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR))
        .isEqualTo(AuthenticatorType.SPCS.toConfigValue());
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME))
        .isEqualTo("myaccount.dep.us-west-2.aws.snowflakecomputing.com");
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME))
        .isEqualTo("AMBIENT_DB");
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME))
        .isEqualTo("AMBIENT_SCHEMA");
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME))
        .isEqualTo(SpcsEnvironment.AMBIENT_USER_PLACEHOLDER);
  }

  @Test
  public void startLeavesConfigUntouchedOutsideSpcs(@TempDir Path tempDir) {
    // GIVEN an environment that is not SPCS: no host, and a token path that does not exist
    SpcsEnvironment.overrideForTests(name -> null, tempDir.resolve("absent"));

    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.NAME, "ordinary_connector");
    raw.put(KafkaConnectorConfigParams.TOPICS, "t1");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "SOME_ROLE");

    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector();
    try {
      connector.start(raw);
    } catch (Throwable expected) {
      // ignored on purpose
    }

    Map<String, String> effective = connector.effectiveConfigForTests();
    assertThat(effective).isNotNull();
    // No authenticator was configured and none may be invented outside SPCS. Defaults applied by
    // ConnectorConfigTools may set one, so assert only that it is not spcs.
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR))
        .isNotEqualTo(AuthenticatorType.SPCS.toConfigValue());
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME)).isNull();
    assertThat(effective.get(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME)).isNull();
  }

  /**
   * Regression test for a defect found only by running the connector under a real Kafka Connect
   * worker inside SPCS.
   *
   * <p>Kafka Connect's herder calls {@link SnowflakeStreamingSinkConnector#validate} <b>before</b>
   * {@code start()}. Resolution therefore has to happen in {@code validate()} too: without it, the
   * single-field checks in {@code Utils.isSingleFieldValid} run against the raw configuration and
   * reject a legitimate credential-free SPCS config with four errors:
   *
   * <pre>
   *   snowflake.url.name must be provided
   *   snowflake.user.name must be provided
   *   snowflake.database.name must be provided
   *   snowflake.schema.name must be provided
   * </pre>
   *
   * <p>Connector creation then fails outright, so the feature is unusable from Kafka Connect even
   * though every in-process test passes. The earlier unit tests could not catch this because they
   * drive {@code start()} and {@code SinkTaskConfig} directly and never go through the herder.
   *
   * <p>A live connection attempt later in {@code validate()} cannot succeed in a unit test, so this
   * asserts specifically on the absence of the "must be provided" errors rather than on the absence
   * of all errors. A "Cannot connect to Snowflake" message is expected and is not a regression.
   */
  @Test
  public void validateResolvesAmbientCredentialsBecauseConnectValidatesBeforeStart(
      @TempDir Path tempDir) throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, "my-account.dep.us-west-2.aws.snowflakecomputing.com");
    env.put("SNOWFLAKE_DATABASE", "AMBIENT_DB");
    env.put("SNOWFLAKE_SCHEMA", "AMBIENT_SCHEMA");
    SpcsEnvironment.overrideForTests(env::get, token);

    // A credential-free configuration, exactly as a Kafka Connect user would write it in SPCS.
    Map<String, String> raw = new HashMap<>();
    raw.put(KafkaConnectorConfigParams.TOPICS, "t1");
    raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "SOME_ROLE");

    org.apache.kafka.common.config.Config result =
        new SnowflakeStreamingSinkConnector().validate(raw);

    for (org.apache.kafka.common.config.ConfigValue value : result.configValues()) {
      assertThat(value.errorMessages())
          .as(
              "%s must be filled in from the SPCS runtime during validate(), because Kafka Connect"
                  + " validates before it starts the connector",
              value.name())
          .noneMatch(message -> message.contains("must be provided"));
    }
  }
}
