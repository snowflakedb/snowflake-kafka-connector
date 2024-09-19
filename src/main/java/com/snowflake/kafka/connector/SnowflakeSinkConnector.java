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

import com.snowflake.kafka.connector.config.ConnectorConfigDefinition;
import com.snowflake.kafka.connector.config.IcebergConfigValidator;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * SnowflakeSinkConnector implements SinkConnector for Kafka Connect framework.
 *
 * <p>Expected configuration: including topic names, partition numbers, snowflake connection info
 * and credentials info
 *
 * <p>Creates snowflake internal stages, snowflake tables provides configuration to SinkTasks
 * running on Kafka Connect Workers.
 */
public class SnowflakeSinkConnector extends SinkConnector {
  // create logger without correlationId for now
  private static KCLogger LOGGER = new KCLogger(SnowflakeSinkConnector.class.getName());

  private Map<String, String> config; // connector configuration, provided by
  // user through kafka connect framework

  // SnowflakeJDBCWrapper provides methods to interact with user's snowflake
  // account and executes queries
  private SnowflakeConnectionService conn;

  // Snowflake Telemetry provides methods to report usage statistics
  private SnowflakeTelemetryService telemetryClient;
  private long connectorStartTime;

  // Kafka Connect starts sink tasks without waiting for setup in
  // SnowflakeSinkConnector to finish.
  // This causes race conditions for: config validation, tables and stages
  // creation, etc.
  // Using setupComplete to synchronize
  private boolean setupComplete;

  private final ConnectorConfigValidator connectorConfigValidator =
      new DefaultConnectorConfigValidator(
          new DefaultStreamingConfigValidator(), new IcebergConfigValidator());

  /** No-Arg constructor. Required by Kafka Connect framework */
  public SnowflakeSinkConnector() {
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
    LOGGER.info("SnowflakeSinkConnector:starting...");

    Utils.checkConnectorVersion();

    setupComplete = false;
    connectorStartTime = System.currentTimeMillis();
    config = new HashMap<>(parsedConfig);

    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    // modify invalid connector name
    Utils.convertAppName(config);

    connectorConfigValidator.validateConfig(config);

    // enable mdc logging if needed
    KCLogger.toggleGlobalMdcLoggingContext(
        Boolean.parseBoolean(
            config.getOrDefault(
                SnowflakeSinkConnectorConfig.ENABLE_MDC_LOGGING_CONFIG,
                SnowflakeSinkConnectorConfig.ENABLE_MDC_LOGGING_DEFAULT)));

    // enable proxy
    Utils.enableJVMProxy(config);

    // create a persisted connection, and validate snowflake connection
    // config as a side effect
    conn = SnowflakeConnectionServiceFactory.builder().setProperties(config).build();

    telemetryClient = conn.getTelemetryClient();

    telemetryClient.reportKafkaConnectStart(connectorStartTime, this.config);

    setupComplete = true;

    LOGGER.info("SnowflakeSinkConnector:started");
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
    setupComplete = false;
    LOGGER.info("SnowflakeSinkConnector:stopped");
    telemetryClient.reportKafkaConnectStop(connectorStartTime);
  }

  // TODO (post GA): override reconfigure(java.util.Map<java.lang.String,java
  // .lang.String> props)
  // Default implementation shuts down all external network connections.
  // We can make it more efficient by identifying configuration changes,
  // creating new snowflake internal stages, new snowflake tables, new pipes,
  // for newly added topics;
  // and cleaning up stages for topics that are not in the new configuration,
  // and
  // cleaning up pipes for partitions that are not in the new configuration.

  /** @return Sink task class */
  @Override
  public Class<? extends Task> taskClass() {
    return SnowflakeSinkTask.class;
  }

  /**
   * taskConfigs method returns a set of configurations for SinkTasks based on the current
   * configuration, producing at most 'maxTasks' configurations
   *
   * @param maxTasks maximum number of SinkTasks for this instance of SnowflakeSinkConnector
   * @return a list containing 'maxTasks' copies of the configuration
   */
  @Override
  public List<Map<String, String>> taskConfigs(final int maxTasks) {
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

  /** @return ConfigDef with original configuration properties */
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

    // If using snowflake_jwt and authentication, and private key or private key passphrase is
    // provided through a config provider, skip validation
    if (isUsingJWT(connectorConfigs) && isUsingConfigProvider(connectorConfigs)) {
      return result;
    }

    // We don't validate name, since it is not included in the return value
    // so just put a test connector here
    connectorConfigs.put(Utils.NAME, "TEST_CONNECTOR");
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
          Utils.updateConfigErrorMessage(result, Utils.SF_URL, ": Cannot connect to Snowflake");
          Utils.updateConfigErrorMessage(
              result, Utils.SF_PRIVATE_KEY, ": Cannot connect to Snowflake");
          Utils.updateConfigErrorMessage(result, Utils.SF_USER, ": Cannot connect to Snowflake");
          break;
        case "0007":
          Utils.updateConfigErrorMessage(result, Utils.SF_URL, " is not a valid snowflake url");
          break;
        case "0018":
          Utils.updateConfigErrorMessage(result, Utils.PRIVATE_KEY_PASSPHRASE, " is not valid");
          Utils.updateConfigErrorMessage(result, Utils.SF_PRIVATE_KEY, " is not valid");
          break;
        case "0013":
          Utils.updateConfigErrorMessage(result, Utils.SF_PRIVATE_KEY, " must be non-empty");
          break;
        case "0026":
          Utils.updateConfigErrorMessage(
              result,
              Utils.SF_OAUTH_CLIENT_ID,
              " must be non-empty when using oauth authenticator");
          break;
        case "0027":
          Utils.updateConfigErrorMessage(
              result,
              Utils.SF_OAUTH_CLIENT_SECRET,
              " must be non-empty when using oauth authenticator");
          break;
        case "0028":
          Utils.updateConfigErrorMessage(
              result,
              Utils.SF_OAUTH_REFRESH_TOKEN,
              " must be non-empty when using oauth authenticator");
          break;
        case "0029":
          Utils.updateConfigErrorMessage(
              result, Utils.SF_AUTHENTICATOR, " is not a valid authenticator");
          break;
        case "0002":
          Utils.updateConfigErrorMessage(
              result, Utils.SF_PRIVATE_KEY, " must be a valid PEM RSA private key");
          break;
        default:
          throw e; // Shouldn't reach here, so crash.
      }
      return result;
    }

    try {
      testConnection.databaseExists(connectorConfigs.get(Utils.SF_DATABASE));
    } catch (SnowflakeKafkaConnectorException e) {
      LOGGER.error("Validate Error msg:{}, errorCode:{}", e.getMessage(), e.getCode());
      if (e.getCode().equals("2001")) {
        Utils.updateConfigErrorMessage(result, Utils.SF_DATABASE, " database does not exist");
      } else {
        throw e;
      }
      return result;
    }

    try {
      testConnection.schemaExists(connectorConfigs.get(Utils.SF_SCHEMA));
    } catch (SnowflakeKafkaConnectorException e) {
      LOGGER.error("Validate Error msg:{}, errorCode:{}", e.getMessage(), e.getCode());
      if (e.getCode().equals("2001")) {
        Utils.updateConfigErrorMessage(result, Utils.SF_SCHEMA, " schema does not exist");
      } else {
        throw e;
      }
      return result;
    }

    LOGGER.info("Validated config with no error");
    return result;
  }

  private static boolean isUsingConfigProvider(Map<String, String> connectorConfigs) {
    Pattern configProviderPrefix = Pattern.compile("[$][{][a-zA-Z]+:");

    return configProviderPrefix
            .matcher(connectorConfigs.getOrDefault(Utils.SF_PRIVATE_KEY, ""))
            .find()
        || configProviderPrefix
            .matcher(connectorConfigs.getOrDefault(Utils.PRIVATE_KEY_PASSPHRASE, ""))
            .find();
  }

  private static boolean isUsingJWT(Map<String, String> connectorConfigs) {
    return connectorConfigs
        .getOrDefault(Utils.SF_AUTHENTICATOR, Utils.SNOWFLAKE_JWT)
        .equals(Utils.SNOWFLAKE_JWT);
  }

  /** @return connector version */
  @Override
  public String version() {
    return Utils.VERSION;
  }
}
