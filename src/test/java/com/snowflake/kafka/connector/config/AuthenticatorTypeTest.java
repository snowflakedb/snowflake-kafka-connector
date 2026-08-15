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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class AuthenticatorTypeTest {

  /**
   * Pins the exact set of authenticators whose credential identifies the user. This is the property
   * that suppresses the {@code user} property on both credential paths and exempts {@code
   * ERROR_0016}, so widening it silently would change what the connector sends to Snowflake.
   *
   * <p>If a new ambient authenticator is added, update this set deliberately rather than letting
   * the assertion be relaxed to fit.
   */
  @Test
  void exactlySpcsSuppliesAmbientIdentity() {
    Set<AuthenticatorType> ambient =
        Arrays.stream(AuthenticatorType.values())
            .filter(AuthenticatorType::suppliesAmbientIdentity)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(AuthenticatorType.class)));

    assertThat(ambient).containsExactly(AuthenticatorType.SPCS);
  }

  /**
   * The credential-based authenticators must keep asserting a user. Written as a parameterised test
   * over the enum so that adding a constant without considering this property fails here rather
   * than at a call site.
   */
  @ParameterizedTest
  @EnumSource(
      value = AuthenticatorType.class,
      names = {"SNOWFLAKE_JWT", "OAUTH"})
  void credentialAuthenticatorsDoNotSupplyAmbientIdentity(AuthenticatorType authenticator) {
    assertThat(authenticator.suppliesAmbientIdentity()).isFalse();
  }
}
