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
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class ConnectorConfigTools {

  private static final KCLogger LOGGER = new KCLogger(ConnectorConfigTools.class.getName());

  public static final ConfigDef.Validator BOOLEAN_VALIDATOR =
      new ConfigDef.Validator() {
        private final ConfigDef.ValidString validator =
            ConfigDef.ValidString.in(
                Boolean.TRUE.toString().toLowerCase(Locale.ROOT),
                Boolean.FALSE.toString().toLowerCase(Locale.ROOT));

        @Override
        public void ensureValid(String name, Object value) {
          if (value instanceof String) {
            value = ((String) value).toLowerCase(Locale.ROOT);
          }
          this.validator.ensureValid(name, value);
        }
      };

  public static void setDefaultValues(Map<String, String> config) {
    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG)) {
      config.put(
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG,
          KafkaConnectorConfigParams.SNOWPIPE_STREAMING_MAX_CLIENT_LAG_SECONDS_DEFAULT + "");
      LOGGER.info(
          "{} set to default {} seconds",
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG,
          KafkaConnectorConfigParams.SNOWPIPE_STREAMING_MAX_CLIENT_LAG_SECONDS_DEFAULT);
    }
    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION)) {
      config.put(
          KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION,
          KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION_DEFAULT);
      LOGGER.info(
          "{} set to default {}",
          KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION,
          KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION_DEFAULT);
    }
    if (!config.containsKey(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS)) {
      config.put(
          KafkaConnectorConfigParams.CACHE_TABLE_EXISTS,
          String.valueOf(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_DEFAULT));
      LOGGER.info(
          "{} set to default {}",
          KafkaConnectorConfigParams.CACHE_TABLE_EXISTS,
          KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_DEFAULT);
    }
    if (!config.containsKey(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS)) {
      config.put(
          KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS,
          String.valueOf(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT));
      LOGGER.info(
          "{} set to default {} ms",
          KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS,
          KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT);
    }
    if (!config.containsKey(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS)) {
      config.put(
          KafkaConnectorConfigParams.CACHE_PIPE_EXISTS,
          String.valueOf(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_DEFAULT));
      LOGGER.info(
          "{} set to default {}",
          KafkaConnectorConfigParams.CACHE_PIPE_EXISTS,
          KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_DEFAULT);
    }
    if (!config.containsKey(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS)) {
      config.put(
          KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS,
          String.valueOf(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT));
      LOGGER.info(
          "{} set to default {} ms",
          KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS,
          KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT);
    }
  }

  /**
   * Get a property from the config map
   *
   * @param config connector configuration
   * @param key name of the key to be retrieved
   * @return property value or null
   */
  public static String getProperty(final Map<String, String> config, final String key) {
    if (config.containsKey(key) && !config.get(key).isEmpty()) {
      return config.get(key);
    } else {
      return null;
    }
  }

  /* The allowed values for tombstone records. */
  public enum BehaviorOnNullValues {
    // Default as the name suggests, would be a default behavior which will not filter null values.
    // This will put an empty JSON string in corresponding snowflake table.
    // Using this means we will fall back to old behavior before introducing this config.
    DEFAULT,

    // Ignore would filter out records which has null value, but a valid key.
    IGNORE,
    ;

    /* Validator to validate behavior.on.null.values which says whether kafka should keep null value records or ignore them while ingesting into snowflake table. */
    public static final ConfigDef.Validator VALIDATOR =
        new ConfigDef.Validator() {
          private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

          @Override
          public void ensureValid(String name, Object value) {
            if (value instanceof String) {
              value = ((String) value).toLowerCase(Locale.ROOT);
            }
            validator.ensureValid(name, value);
          }

          // Overridden here so that ConfigDef.toEnrichedRst shows possible values correctly
          @Override
          public String toString() {
            return validator.toString();
          }
        };

    // All valid enum values
    public static String[] names() {
      BehaviorOnNullValues[] behaviors = values();
      String[] result = new String[behaviors.length];

      for (int i = 0; i < behaviors.length; i++) {
        result[i] = behaviors[i].toString();
      }

      return result;
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  /* https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/ */
  public enum ErrorTolerance {

    /** Tolerate no errors. */
    NONE,

    /** Tolerate all errors. */
    ALL;

    /**
     * Validator to validate behavior.on.null.values which says whether kafka should keep null value
     * records or ignore them while ingesting into snowflake table.
     */
    public static final ConfigDef.Validator VALIDATOR =
        new ConfigDef.Validator() {
          private final ConfigDef.ValidString validator =
              ConfigDef.ValidString.in(ErrorTolerance.names());

          @Override
          public void ensureValid(String name, Object value) {
            if (value instanceof String) {
              value = ((String) value).toLowerCase(Locale.ROOT);
            }
            validator.ensureValid(name, value);
          }

          @Override
          public String toString() {
            return validator.toString();
          }
        };

    /** @return All valid enum values */
    public static String[] names() {
      ErrorTolerance[] errorTolerances = values();
      String[] result = new String[errorTolerances.length];

      for (int i = 0; i < errorTolerances.length; i++) {
        result[i] = errorTolerances[i].toString();
      }

      return result;
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }
}
