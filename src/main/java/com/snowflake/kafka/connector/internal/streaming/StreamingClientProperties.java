/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
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

package com.snowflake.kafka.connector.internal.streaming;

import com.google.common.base.Strings;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.AuthenticatorType;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.PrivateKeyTool;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.spcs.SpcsEnvironment;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Object to convert and store properties for {@code
 * net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient}. This object is used to compare
 * equality between clients in {@code StreamingClientProvider}.
 */
public class StreamingClientProperties {
  public static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  public static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";

  /**
   * Identifier sent to the SSv2 SDK via the {@code application} property. The SDK appends it to the
   * outbound User-Agent as {@code (app=<value>)} for connector version attribution.
   */
  public static final String APPLICATION_NAME = "SnowflakeKafkaConnector/" + Utils.VERSION;

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProperties.class.getName());
  public final Properties clientProperties;
  public final String clientNamePrefix;
  public final Map<String, Object> parameterOverrides;

  /** Constructor used by {@link #from(SinkTaskConfig)}. */
  private StreamingClientProperties(
      Properties clientProperties,
      String clientNamePrefix,
      Map<String, Object> parameterOverrides) {
    this.clientProperties = clientProperties;
    this.clientNamePrefix = clientNamePrefix;
    this.parameterOverrides = parameterOverrides;
  }

  /** Creates streaming client properties from parsed {@link SinkTaskConfig}. */
  public static StreamingClientProperties from(SinkTaskConfig config) {
    final Properties clientProperties = new Properties();
    if (Strings.isNullOrEmpty(config.getSnowflakeUrl())) {
      // Without a URL there is no account or host: the authorization type and credential would be
      // pointless on their own, and the SDK would fail with an unrelated error. Validated
      // configurations cannot reach this branch; DefaultConnectorConfigValidator rejects a blank
      // snowflake.url.name. Reaching here means a SinkTaskConfig was built without validation.
      throw SnowflakeErrors.ERROR_0017.getException();
    }
    SnowflakeURL url = new SnowflakeURL(config.getSnowflakeUrl());
    switch (config.getAuthenticator()) {
      case OAUTH:
        clientProperties.put("authorization_type", AuthenticatorType.OAUTH.toConfigValue());
        config.getOauthClientId().ifPresent(v -> clientProperties.put("oauth_client_id", v));
        config
            .getOauthClientSecret()
            .ifPresent(v -> clientProperties.put("oauth_client_secret", v.value()));
        config
            .getOauthRefreshToken()
            .ifPresent(v -> clientProperties.put("oauth_refresh_token", v.value()));
        config
            .getOauthTokenEndpoint()
            .ifPresent(v -> clientProperties.put("oauth_token_endpoint", v));
        // The SDK defaults oauth_include_scope to true, which makes it derive
        // "session:role:{role}" and send it as the scope on refresh. That breaks external OAuth /
        // IdP refresh tokens minted without a role scope, so forward the connector's value
        // explicitly (default false). Only send an explicit scope when scopes are enabled.
        clientProperties.put("oauth_include_scope", String.valueOf(config.getOauthIncludeScope()));
        if (config.getOauthIncludeScope()) {
          config.getOauthScope().ifPresent(v -> clientProperties.put("oauth_scope", v));
        }
        break;
      case SNOWFLAKE_JWT:
        final PrivateKey privateKey =
            PrivateKeyTool.parsePrivateKey(
                config
                    .getSnowflakePrivateKey()
                    .orElseThrow(SnowflakeErrors.ERROR_0013::getException),
                config.getSnowflakePrivateKeyPassphrase());
        final String privateKeyEncoded =
            Base64.getEncoder().encodeToString(privateKey.getEncoded());
        clientProperties.put("private_key", privateKeyEncoded);
        break;
      case SPCS:
        // Snowflake-provided service user credentials ("ambient" authentication). Deliberately
        // sets no spcs_* keys: the SDK already defaults spcs_token_path and
        // spcs_service_token_path to the standard SPCS locations, and its exclusive-field check
        // treats them as user-set only when they differ from those defaults. Leaving them alone
        // also keeps the connector clear of the config-key injection path tracked in
        // SNOW-3686036. Note no oauth_* key may be set here, including oauth_include_scope, which
        // the SDK counts as set when false.
        clientProperties.put("authorization_type", AuthenticatorType.SPCS.toConfigValue());
        break;
      default:
        throw new IllegalStateException("unhandled authenticator: " + config.getAuthenticator());
    }

    if (!config.getAuthenticator().suppliesAmbientIdentity()) {
      // See the note in InternalUtils.makeJdbcDriverProperties: when the credential itself
      // identifies the user, asserting any other user is rejected. The connector cannot know
      // the generated service user's name, so it must not send one.
      clientProperties.put("user", config.getSnowflakeUser());
    }
    // A role is optional under SPCS workload-identity authentication; omit it rather than send
    // null so the service user's default (service owner) role applies.
    String role = config.getSnowflakeRole();
    if (role != null && !role.trim().isEmpty()) {
      clientProperties.put("role", role);
    }
    // For ambient SPCS authentication, use SNOWFLAKE_ACCOUNT from the environment
    // rather than parsing the first label of the host URL.  The host is an undocumented
    // format verified on one deployment; SNOWFLAKE_ACCOUNT is the canonical identifier
    // published by every SPCS runtime and is how the Snowflake CLI establishes its account.
    // If the environment variable is absent (not inside SPCS) the parsed value is kept.
    String account = url.getAccount();
    if (config.getAuthenticator().suppliesAmbientIdentity()) {
      account =
          SpcsEnvironment.account()
              .orElseGet(
                  () -> {
                    // SNOWFLAKE_ACCOUNT is published by SPCS runtimes at release 8.35 and later.
                    // Fall back to parsing the first label of the host URL for older deployments.
                    LOGGER.warn(
                        "SNOWFLAKE_ACCOUNT environment variable is absent; parsing account from"
                            + " host '{}'. If the connector is running in SPCS, upgrade the"
                            + " service runtime to 8.35 or later to supply the canonical account"
                            + " identifier.",
                        url.getUrlWithoutPort());
                    return url.getAccount();
                  });
    }
    clientProperties.put("account", account);
    clientProperties.put("host", url.getUrlWithoutPort());
    clientProperties.put("application", APPLICATION_NAME);

    // Passwords stay on clientProperties, which is not logged; only parameterOverrides is logged
    // below.
    putProxy(clientProperties, config);

    String clientNamePrefix =
        STREAMING_CLIENT_V2_PREFIX_NAME
            + (config.getConnectorName() != null ? config.getConnectorName() : DEFAULT_CLIENT_NAME);

    Map<String, Object> parameterOverrides = new HashMap<>();
    String overrideMap = config.getStreamingClientProviderOverrideMap();
    if (overrideMap != null && !overrideMap.isEmpty()) {
      Utils.parseCommaSeparatedKeyValuePairs(overrideMap)
          .forEach((key, value) -> parameterOverrides.put(key.toLowerCase(), value));
      LOGGER.info("Streaming Client config overrides: {}", parameterOverrides);
    }

    return new StreamingClientProperties(clientProperties, clientNamePrefix, parameterOverrides);
  }

  /**
   * Sends the SDK the same proxy the connector's JDBC and OAuth clients use: {@code jvm.proxy.*}
   * when host and port are set, else the JVM's {@code https.proxyHost} (then {@code
   * http.proxyHost}) system properties, else nothing, so the SDK falls back to {@code HTTPS_PROXY}
   * / {@code NO_PROXY}. The bypass list is passed unchanged: the SDK reads the JDK {@code
   * nonProxyHosts} format.
   */
  private static void putProxy(Properties clientProperties, SinkTaskConfig config) {
    String url = proxyUrl(config.getProxyHost(), config.getProxyPort());
    String noProxy;
    String user;
    String password;
    if (url != null) {
      noProxy = config.getNonProxyHosts();
      user = config.getProxyUsername();
      password = config.getProxyPassword();
    } else {
      String httpsUrl =
          proxyUrl(
              System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_HOST),
              System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PORT));
      if (httpsUrl != null) {
        url = httpsUrl;
        user = System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_USER);
        password = System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD);
      } else {
        url =
            proxyUrl(
                System.getProperty(KafkaConnectorConfigParams.HTTP_PROXY_HOST),
                System.getProperty(KafkaConnectorConfigParams.HTTP_PROXY_PORT));
        user = System.getProperty(KafkaConnectorConfigParams.HTTP_PROXY_USER);
        password = System.getProperty(KafkaConnectorConfigParams.HTTP_PROXY_PASSWORD);
      }
      if (url == null) {
        return;
      }
      noProxy = System.getProperty(KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS);
    }
    clientProperties.put("https_proxy_url", url);
    if (blankToNull(noProxy) != null) {
      clientProperties.put("no_proxy", noProxy.trim());
    }
    if (blankToNull(user) != null && blankToNull(password) != null) {
      clientProperties.put("proxy_user", user);
      clientProperties.put("proxy_password", password);
    }
  }

  /** {@code http://host:port}. Returns null when host or port is missing. */
  static String proxyUrl(String host, String port) {
    host = blankToNull(host);
    port = blankToNull(port);
    if (host == null || port == null) {
      return null;
    }
    host = host.trim();
    port = port.trim();
    if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
      host = "[" + host + "]";
    }
    return "http://" + host + ":" + port;
  }

  private static String blankToNull(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    return value;
  }

  /**
   * Determines equality between StreamingClientProperties by only looking at the parsed
   * clientProperties. This is used in {@code StreamingClientProvider} to determine equality in
   * registered clients
   *
   * @param other other object to determine equality
   * @return if the given object's clientProperties exists and is equal
   */
  @Override
  public boolean equals(Object other) {
    return other.getClass().equals(StreamingClientProperties.class)
        && ((StreamingClientProperties) other).clientProperties.equals(this.clientProperties)
        && ((StreamingClientProperties) other).parameterOverrides.equals(this.parameterOverrides);
  }

  /**
   * Creates the hashcode for this object from the clientProperties. This is used in {@code
   * StreamingClientProvider} to determine equality in registered clients
   *
   * @return the clientProperties' hashcode
   */
  @Override
  public int hashCode() {
    return Objects.hash(this.clientProperties, this.parameterOverrides);
  }
}
