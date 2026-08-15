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
package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.spcs.SpcsEnvironment;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Whole-key-set regression guard for the credential-based path to the JDBC driver.
 *
 * <p>The ambient SPCS change touches shared code on the way to the driver: it makes the {@code
 * user} property conditional and exempts one validation gate. The surrounding suites assert
 * individual properties, which proves the new behavior but would not notice a property quietly
 * appearing or disappearing for {@code snowflake_jwt}.
 *
 * <p>This pins the <b>entire</b> key set, so any future change that adds, removes or renames a JDBC
 * property on the key-pair path fails here and has to be justified. The equivalent whole-map check
 * for the streaming path already exists in {@code StreamingClientPropertiesTest}; the OAuth JDBC
 * path deliberately has no unit-level equivalent because building it calls {@code
 * OAuthAccessTokenFetcher} and would require a live token endpoint.
 */
public class InternalUtilsNonSpcsRegressionTest {

  @AfterEach
  public void reset() {
    SpcsEnvironment.resetForTests();
  }

  @Test
  public void keyPairJdbcPropertyKeySetIsUnchanged() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    SnowflakeURL url = TestUtils.getUrl();

    Properties props =
        InternalUtils.makeJdbcDriverProperties(SinkTaskConfig.from(config, true), url);

    // keySet(), not stringPropertyNames(): the latter silently omits entries whose value is not a
    // String, and "privateKey" holds a java.security.PrivateKey. Using stringPropertyNames() here
    // would make this guard blind to exactly the property that matters most on this path.
    TreeSet<String> keys = new TreeSet<>();
    props.keySet().forEach(k -> keys.add(String.valueOf(k)));

    assertThat(keys)
        .as(
            "the JDBC property key set for the key-pair path must not change; ambient SPCS support"
                + " must not add or remove properties here")
        .containsExactly(
            "JDBC_QUERY_RESULT_FORMAT",
            "allowUnderscoresInHost",
            "authenticator",
            "client_session_keep_alive",
            "db",
            "privateKey",
            "role",
            "schema",
            "ssl",
            "user");

    // The two properties the ambient change is most likely to have disturbed.
    assertThat(props.getProperty("authenticator")).isEqualTo("snowflake_jwt");
    assertThat(keys)
        .as("a key-pair connection must never carry an ambient bearer token")
        .doesNotContain("token");
  }
}
