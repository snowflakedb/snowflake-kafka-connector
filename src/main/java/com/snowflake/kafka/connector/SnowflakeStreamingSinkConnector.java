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

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.AuthenticatorType;
import com.snowflake.kafka.connector.config.ConnectorConfigDefinition;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryLevel;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * SnowflakeStreamingSinkConnector implements SinkConnector for Kafka Connect framework.
 *
 * <p>Expected configuration: including topic names, partition numbers, snowflake connection info
 * and credentials info
 *
 * <p>Creates snowflake internal stages, snowflake tables provides configuration to SinkTasks
 * running on Kafka Connect Workers.
 */
public class SnowflakeStreamingSinkConnector extends SinkConnector {
  // create logger without correlationId for now
  private static final KCLogger LOGGER =
      new KCLogger(SnowflakeStreamingSinkConnector.class.getName());

  // Matches a Kafka Connect config-provider reference such as ${vault:...} or ${file:...}, whose
  // value is resolved per-worker at task start and is therefore not available during validate().
  private static final Pattern CONFIG_PROVIDER_PREFIX = Pattern.compile("[$][{][a-zA-Z]+:");

  private Map<String, String> config; // connector configuration, provided by
  // user through kafka connect framework

  // SnowflakeJDBCWrapper provides methods to interact with user's snowflake
  // account and executes queries
  private SnowflakeConnectionService conn;

  // Snowflake Telemetry provides methods to report usage statistics
  private SnowflakeTelemetryService telemetryClient;
  private long connectorStartTime;

  // Kafka Connect starts sink tasks without waiting for setup in
  // SnowflakeStreamingSinkConnector to finish.
  // This causes race conditions for: config validation, tables and stages
  // creation, etc.
  // Using setupComplete to synchronize
  private boolean setupComplete;

  private final ConnectorConfigValidator connectorConfigValidator =
      new DefaultConnectorConfigValidator(new DefaultStreamingConfigValidator());

  // Periodically re-polls server advisories while the connector runs (SNOW-3648284). Unlike the
  // one-time startup check, this is log-only and never hard-fails a running connector.
  private ScheduledExecutorService advisoryPoller;

  // test visibility: counts completed periodic poll cycles
  private final AtomicInteger advisoryPollCount = new AtomicInteger();

  /** No-Arg constructor. Required by Kafka Connect framework */
  public SnowflakeStreamingSinkConnector() {
    setupComplete = false;
  }

  /**
   * start method will only be called on a clean connector, i.e. it has either just been
   * instantiated and initialized or stop () has been invoked. loads configuration and validates.
   *
   * <p>Creates snowflake internal stages and snowflake tables
   *
   * @param parsedConfig has the configuration settings
   */
  @Override
  public void start(final Map<String, String> parsedConfig) {
    LOGGER.info("SnowflakeStreamingSinkConnector:starting...");

    Utils.checkConnectorVersion();

    setupComplete = false;
    connectorStartTime = System.currentTimeMillis();
    config = new HashMap<>(parsedConfig);

    ConnectorConfigTools.setDefaultValues(config);

    // modify invalid connector name
    Utils.convertAppName(config);

    connectorConfigValidator.validateConfig(config);

    // enable mdc logging if needed
    KCLogger.toggleGlobalMdcLoggingContext(
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.ENABLE_MDC_LOGGING_CONFIG,
                KafkaConnectorConfigParams.ENABLE_MDC_LOGGING_DEFAULT)));

    // enable proxy
    Utils.enableJVMProxy(config);

    // create a persisted connection, and validate snowflake connection
    // config as a side effect
    conn = SnowflakeConnectionServiceFactory.builder().setProperties(config).build();

    // Fetch and log any server-side advisories for this connector version (SNOW-3648284).
    logServerAdvisories();
    startAdvisoryPolling();

    telemetryClient = conn.getTelemetryClient();

    telemetryClient.reportKafkaConnectStart(connectorStartTime, this.config);

    setupComplete = true;

    LOGGER.info("SnowflakeStreamingSinkConnector:started");
  }

  /**
   * Stop method will be called to stop a connector, cleans up snowflake internal stages, after
   * making sure that there are no pending files to ingest.
   *
   * <p>Cleans up pipes, after making sure there are no pending files to ingest.
   *
   * <p>Also ensures that there are no leaked stages, no leaked staged files, and no leaked pipes
   */
  @Override
  public void stop() {
    LOGGER.info("SnowflakeStreamingSinkConnector connector stopping...");
    setupComplete = false;

    if (advisoryPoller != null) {
      advisoryPoller.shutdownNow();
    }

    if (telemetryClient != null) {
      telemetryClient.reportKafkaConnectStop(connectorStartTime);
    }
  }

  /**
   * @return Sink task class
   */
  @Override
  public Class<? extends Task> taskClass() {
    return SnowflakeSinkTask.class;
  }

  /**
   * taskConfigs method returns a set of configurations for SinkTasks based on the current
   * configuration, producing at most 'maxTasks' configurations
   *
   * @param maxTasks maximum number of SinkTasks for this instance of
   *     SnowflakeStreamingSinkConnector
   * @return a list containing 'maxTasks' copies of the configuration
   */
  @Override
  public List<Map<String, String>> taskConfigs(final int maxTasks) {
    LOGGER.info("taskConfigs called with maxTasks: {}", maxTasks);
    // wait for setup to complete
    int counter = 0;
    while (counter < 120) // poll for 120*5 seconds (10 mins) maximum
    {
      if (setupComplete) {
        break;
      } else {
        counter++;
        try {
          LOGGER.info("Sleeping 5000ms to allow setup to " + "complete.");
          Thread.sleep(5000);
        } catch (InterruptedException ex) {
          LOGGER.warn("Waiting for setup to complete got " + "interrupted");
        }
      }
    }
    if (!setupComplete) {
      throw SnowflakeErrors.ERROR_5007.getException(telemetryClient);
    }

    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> conf = new HashMap<>(config);
      conf.put(Utils.TASK_ID, i + "");
      taskConfigs.add(conf);
    }
    return taskConfigs;
  }

  /**
   * @return ConfigDef with original configuration properties
   */
  @Override
  public ConfigDef config() {
    return ConnectorConfigDefinition.getConfig();
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    LOGGER.debug("Validating connector Config: Start");
    // cross-fields validation here
    Config result = super.validate(connectorConfigs);

    // Validate ensure that url, user, db, schema, private key exist in config and is not empty
    // and there is no single field validation error
    if (!Utils.isSingleFieldValid(result)) {
      return result;
    }

    // Verify proxy config is valid
    Map<String, String> invalidProxyParams = Utils.validateProxySettings(connectorConfigs);

    for (String invalidKey : invalidProxyParams.keySet()) {
      Utils.updateConfigErrorMessage(result, invalidKey, invalidProxyParams.get(invalidKey));
    }

    // Skip live connection validation only when a credential is supplied through a config provider
    // (private key/passphrase or an OAuth credential); its placeholder cannot be resolved here.
    if (isUsingConfigProviderForPrivateKey(connectorConfigs)
        || isUsingConfigProviderForOAuth(connectorConfigs)) {
      return result;
    }

    // Apply proxy settings before the test connection so the OAuth token fetch (which uses the JVM
    // default ProxySelector) sees the same proxy/nonProxyHosts config as runtime does in start().
    Utils.enableJVMProxy(connectorConfigs);

    // We don't validate name, since it is not included in the return value
    // so just put a test connector here
    connectorConfigs.put(KafkaConnectorConfigParams.NAME, "TEST_CONNECTOR");
    SnowflakeConnectionService testConnection;
    try {
      testConnection =
          SnowflakeConnectionServiceFactory.builder().setProperties(connectorConfigs).build();
    } catch (SnowflakeKafkaConnectorException e) {
      LOGGER.error(
          "Validate: Error connecting to snowflake:{}, errorCode:{}", e.getMessage(), e.getCode());
      // Since url, user, db, schema, exist in config and is not empty,
      // the exceptions here would be invalid URL, and cannot connect, and no private key
      switch (e.getCode()) {
        case "1001":
          // Could be caused by invalid url, invalid user name, invalid password.
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
              ": Cannot connect to Snowflake");
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
              ": Cannot connect to Snowflake");
          // A 1001 for OAuth means the token fetch already succeeded (a fetch failure surfaces as
          // 1004), so the private key -- which OAuth users do not configure -- is not the culprit.
          if (!isUsingOAuth(connectorConfigs)) {
            Utils.updateConfigErrorMessage(
                result,
                KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
                ": Cannot connect to Snowflake");
          }
          break;
        case "0007":
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
              " is not a valid snowflake url");
          break;
        case "0018":
          Utils.updateConfigErrorMessage(
              result, KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, " is not valid");
          Utils.updateConfigErrorMessage(
              result, KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, " is not valid");
          break;
        case "0013":
          Utils.updateConfigErrorMessage(
              result, KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, " must be non-empty");
          break;
        case "0002":
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
              " must be a valid PEM RSA private key");
          break;
        case "0026":
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID,
              " must be non-empty when using oauth authenticator");
          break;
        case "0027":
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET,
              " must be non-empty when using oauth authenticator");
          break;
        case "0029":
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
              " is not a valid authenticator");
          break;
        case "0033":
          Utils.updateConfigErrorMessage(
              result,
              KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT,
              " is not a valid OAuth token endpoint URL");
          break;
        case "1004":
          {
            // The token fetch can fail for a bad client id/secret, a wrong token endpoint, or a
            // transient network/proxy issue - flag the whole OAuth credential set.
            String tokenFetchError =
                ": Could not fetch OAuth access token. Check the client ID, client secret, and"
                    + " token endpoint.";
            Utils.updateConfigErrorMessage(
                result, KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID, tokenFetchError);
            Utils.updateConfigErrorMessage(
                result, KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET, tokenFetchError);
            Utils.updateConfigErrorMessage(
                result, KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT, tokenFetchError);
          }
          break;
        default:
          throw e; // Shouldn't reach here, so crash.
      }
      return result;
    }

    try {
      testConnection.databaseExists(
          connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME));
    } catch (SnowflakeKafkaConnectorException e) {
      LOGGER.error("Validate Error msg:{}, errorCode:{}", e.getMessage(), e.getCode());
      if (e.getCode().equals("2001")) {
        Utils.updateConfigErrorMessage(
            result, KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME, " database does not exist");
      } else {
        throw e;
      }
      return result;
    }

    try {
      testConnection.schemaExists(
          connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME));
    } catch (SnowflakeKafkaConnectorException e) {
      LOGGER.error("Validate Error msg:{}, errorCode:{}", e.getMessage(), e.getCode());
      if (e.getCode().equals("2001")) {
        Utils.updateConfigErrorMessage(
            result, KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME, " schema does not exist");
      } else {
        throw e;
      }
      return result;
    }

    LOGGER.info("Validated config with no error");
    return result;
  }

  private static boolean isUsingOAuth(Map<String, String> connectorConfigs) {
    try {
      return AuthenticatorType.fromConfig(
              connectorConfigs.getOrDefault(
                  KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
                  AuthenticatorType.SNOWFLAKE_JWT.toConfigValue()))
          == AuthenticatorType.OAUTH;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static boolean isUsingConfigProviderForPrivateKey(Map<String, String> connectorConfigs) {
    return isConfigProviderReference(
            connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY))
        || isConfigProviderReference(
            connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE));
  }

  private static boolean isUsingConfigProviderForOAuth(Map<String, String> connectorConfigs) {
    return isConfigProviderReference(
            connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID))
        || isConfigProviderReference(
            connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET))
        || isConfigProviderReference(
            connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_REFRESH_TOKEN))
        || isConfigProviderReference(
            connectorConfigs.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT));
  }

  private static boolean isConfigProviderReference(String value) {
    return value != null && CONFIG_PROVIDER_PREFIX.matcher(value).find();
  }

  /**
   * Fetches server-side advisory messages for this connector version, logs each one at its declared
   * level, and fails startup if any are CRITICAL (unless the user has opted out).
   *
   * <p>Mechanism errors (old GS without the function, JDBC/parse failures) are silently swallowed
   * and never block startup; {@link
   * com.snowflake.kafka.connector.internal.SnowflakeConnectionService#getKcAdvisoryMessages} is
   * already fail-safe and returns an empty list on any such error.
   */
  private void logServerAdvisories() {
    List<AdvisoryMessage> advisories;
    try {
      String requestJson = "{\"connectorVersion\":\"" + Utils.VERSION + "\"}";
      advisories = conn.getKcAdvisoryMessages(requestJson); // fail-safe: [] on any error
    } catch (Exception e) {
      LOGGER.debug(
          "Failed to fetch server advisories: {}", e.getMessage()); // mechanism error, never fail
      return;
    }
    for (AdvisoryMessage msg : advisories) {
      AdvisoryLevel.fromString(msg.getLevel()).log(LOGGER, msg.getText());
    }
    boolean failOnCritical =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY_DEFAULT)));
    if (hasCritical(advisories) && failOnCritical) {
      throw SnowflakeErrors.ERROR_5031.getException(
          "Refusing to start: a critical advisory applies to Kafka Connector "
              + Utils.VERSION
              + ". Upgrade the connector, or to start anyway set "
              + KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY
              + "=false (not recommended).");
    } else if (hasCritical(advisories)) {
      LOGGER.warn(
          "Starting despite a critical server advisory because {}=false.",
          KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY);
    }
  }

  /**
   * Returns true if any advisory in the list has level CRITICAL.
   *
   * <p>Package-private for testing.
   */
  static boolean hasCritical(List<AdvisoryMessage> advisories) {
    return advisories.stream()
        .anyMatch(m -> AdvisoryLevel.fromString(m.getLevel()) == AdvisoryLevel.CRITICAL);
  }

  /**
   * Re-fetches and logs server-side advisories on a background thread while the connector is
   * running. Unlike {@link #logServerAdvisories()}, this is log-only: a CRITICAL advisory is logged
   * at ERROR but never stops a running connector, and any exception here is swallowed so the
   * scheduled task keeps firing.
   */
  private void pollAdvisories() {
    try {
      String requestJson = "{\"connectorVersion\":\"" + Utils.VERSION + "\"}";
      for (AdvisoryMessage msg : conn.getKcAdvisoryMessages(requestJson)) {
        AdvisoryLevel.fromString(msg.getLevel()).log(LOGGER, msg.getText());
      }
    } catch (Exception e) {
      LOGGER.debug("Periodic advisory poll failed: {}", e.getMessage());
    } finally {
      advisoryPollCount.incrementAndGet();
    }
  }

  /**
   * Starts a background scheduler that periodically re-polls server advisories (SNOW-3648284). No
   * scheduler is started when the poll interval is {@code <= 0}, which leaves advisory checking as
   * startup-only.
   */
  private void startAdvisoryPolling() {
    long intervalSeconds =
        Long.parseLong(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS_DEFAULT)));
    if (intervalSeconds <= 0) {
      LOGGER.info("Server advisory polling disabled (interval <= 0).");
      return;
    }
    advisoryPoller =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "kc-advisory-poller");
              t.setDaemon(true);
              return t;
            });
    advisoryPoller.scheduleAtFixedRate(
        this::pollAdvisories, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    LOGGER.info("Server advisory polling every {}s.", intervalSeconds);
  }

  /**
   * @return the number of completed periodic advisory poll cycles.
   *     <p>Package-private for testing.
   */
  int advisoryPollCountForTest() {
    return advisoryPollCount.get();
  }

  /**
   * @return connector version
   */
  @Override
  public String version() {
    return Utils.VERSION;
  }
}
