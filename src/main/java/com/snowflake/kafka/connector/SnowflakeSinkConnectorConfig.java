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

import com.google.common.base.Strings;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;

/** A class representing config given by the user */
public class SnowflakeSinkConnectorConfig {

  public static final String NAME = Utils.NAME;
  public static final String TOPICS = "topics";

  // Connector config
  public static final String TOPICS_TABLES_MAP = "snowflake.topic2table.map";

  // For tombstone records
  public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";

  // Snowflake connection and database config
  public static final String SNOWFLAKE_URL = Utils.SF_URL;
  public static final String SNOWFLAKE_USER = Utils.SF_USER;
  public static final String SNOWFLAKE_PRIVATE_KEY = Utils.SF_PRIVATE_KEY;
  public static final String SNOWFLAKE_DATABASE = Utils.SF_DATABASE;
  public static final String SNOWFLAKE_SCHEMA = Utils.SF_SCHEMA;
  public static final String SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = Utils.PRIVATE_KEY_PASSPHRASE;
  public static final String AUTHENTICATOR_TYPE = Utils.SF_AUTHENTICATOR;
  public static final String OAUTH_CLIENT_ID = Utils.SF_OAUTH_CLIENT_ID;
  public static final String OAUTH_CLIENT_SECRET = Utils.SF_OAUTH_CLIENT_SECRET;
  public static final String OAUTH_REFRESH_TOKEN = Utils.SF_OAUTH_REFRESH_TOKEN;
  public static final String OAUTH_TOKEN_ENDPOINT = Utils.SF_OAUTH_TOKEN_ENDPOINT;

  // For Snowpipe Streaming client
  public static final String SNOWFLAKE_ROLE = Utils.SF_ROLE;
  public static final String ENABLE_SCHEMATIZATION_CONFIG = "snowflake.enable.schematization";
  public static final String ENABLE_SCHEMATIZATION_DEFAULT = "false";

  // Proxy Info
  public static final String JVM_PROXY_HOST = "jvm.proxy.host";
  public static final String JVM_PROXY_PORT = "jvm.proxy.port";
  public static final String JVM_NON_PROXY_HOSTS = "jvm.nonProxy.hosts";
  public static final String JVM_PROXY_USERNAME = "jvm.proxy.username";
  public static final String JVM_PROXY_PASSWORD = "jvm.proxy.password";

  // JDBC logging directory Info (environment variable)
  public static final String SNOWFLAKE_JDBC_LOG_DIR = "JDBC_LOG_DIR";

  // JDBC trace Info (environment variable)
  public static final String SNOWFLAKE_JDBC_TRACE = "JDBC_TRACE";

  // JDBC properties map
  public static final String SNOWFLAKE_JDBC_MAP = "snowflake.jdbc.map";

  // Snowflake Metadata Flags
  public static final String SNOWFLAKE_METADATA_CREATETIME = "snowflake.metadata.createtime";
  public static final String SNOWFLAKE_METADATA_TOPIC = "snowflake.metadata.topic";
  public static final String SNOWFLAKE_METADATA_OFFSET_AND_PARTITION =
      "snowflake.metadata.offset.and.partition";
  public static final String SNOWFLAKE_METADATA_ALL = "snowflake.metadata.all";
  public static final String SNOWFLAKE_METADATA_DEFAULT = "true";

  public static final String SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME =
      "snowflake.streaming.metadata.connectorPushTime";
  public static final boolean SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME_DEFAULT = true;

  // Where is Kafka hosted? self, confluent or any other in future.
  // By default it will be None since this is not enforced and only used for monitoring
  public static final String PROVIDER_CONFIG = "provider";

  // metrics
  public static final String JMX_OPT = "jmx";
  public static final boolean JMX_OPT_DEFAULT = true;

  public static final String SNOWPIPE_STREAMING_MAX_CLIENT_LAG =
      "snowflake.streaming.max.client.lag";
  public static final int SNOWPIPE_STREAMING_MAX_CLIENT_LAG_SECONDS_DEFAULT = 1;

  public static final String SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES =
      "snowflake.streaming.max.memory.limit.bytes";
  public static final long SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT = -1L;
  public static final String SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP =
      "snowflake.streaming.client.provider.override.map";

  // Iceberg
  public static final String ICEBERG_ENABLED = "snowflake.streaming.iceberg.enabled";
  public static final boolean ICEBERG_ENABLED_DEFAULT_VALUE = false;

  // TESTING
  public static final String REBALANCING = "snowflake.test.rebalancing";
  public static final boolean REBALANCING_DEFAULT = false;
  // For error handling
  public static final String ERROR_GROUP = "ERRORS";
  public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
  public static final String ERRORS_TOLERANCE_DISPLAY = "Error Tolerance";
  public static final String ERRORS_TOLERANCE_DEFAULT = ErrorTolerance.NONE.toString();
  public static final String ERRORS_TOLERANCE_DOC =
      "Behavior for tolerating errors during Sink connector's operation. 'NONE' is set as default"
          + " and denotes that it will be fail fast. i.e any error will result in an immediate task"
          + " failure. 'ALL'  skips over problematic records.";
  public static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";
  public static final String ERRORS_LOG_ENABLE_DISPLAY = "Log Errors";
  public static final boolean ERRORS_LOG_ENABLE_DEFAULT = false;
  public static final String ERRORS_LOG_ENABLE_DOC =
      "If true, write/log each error along with details of the failed operation and record"
          + " properties to the Connect log. Default is 'false', so that only errors that are not"
          + " tolerated are reported.";
  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG =
      "errors.deadletterqueue.topic.name";
  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DISPLAY =
      "Send error records to the Dead Letter Queue (DLQ)";
  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT = "";
  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DOC =
      "Whether to output conversion errors to the dead letter queue "
          + "By default messages are not sent to the dead letter queue. "
          + "Requires property `errors.tolerance=all`.";
  public static final String ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS =
      "enable.task.fail.on.authorization.errors";
  public static final boolean ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT = false;
  // MDC logging header
  public static final String ENABLE_MDC_LOGGING_CONFIG = "enable.mdc.logging";
  public static final String ENABLE_MDC_LOGGING_DISPLAY = "Enable MDC logging";
  public static final String ENABLE_MDC_LOGGING_DEFAULT = "false";
  public static final String ENABLE_MDC_LOGGING_DOC =
      "Enable MDC context to prepend log messages. Note that this is only available after Apache"
          + " Kafka 2.3";
  /**
   * Used to serialize the incoming records to kafka connector. Note: Converter code is invoked
   * before actually sending records to Kafka connector.
   *
   * @see <a
   *     href="https://github.com/apache/kafka/blob/trunk/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java#L332">Kafka
   *     Code for Converter</a>
   */
  public static final String KEY_CONVERTER_CONFIG_FIELD = "key.converter";

  public static final String VALUE_CONVERTER_CONFIG_FIELD = "value.converter";
  public static final String VALUE_SCHEMA_REGISTRY_CONFIG_FIELD =
      "value.converter.schema.registry.url";
  public static final Set<String> CUSTOM_SNOWFLAKE_CONVERTERS =
      Set.of(
          "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
          "com.snowflake.kafka.connector.records.SnowflakeAvroConverterWithoutSchemaRegistry",
          "com.snowflake.kafka.connector.records.SnowflakeAvroConverter");
  /**
   * Boolean Validator of passed booleans in configurations (TRUE or FALSE). This validator is case
   * insensitive
   */
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

  private static final KCLogger LOGGER = new KCLogger(SnowflakeSinkConnectorConfig.class.getName());

  public static void setDefaultValues(Map<String, String> config) {
    if (!config.containsKey(SNOWPIPE_STREAMING_MAX_CLIENT_LAG)) {
      config.put(
          SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
          SNOWPIPE_STREAMING_MAX_CLIENT_LAG_SECONDS_DEFAULT + "");
      LOGGER.info(
          "{} set to default {} seconds",
          SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
          SNOWPIPE_STREAMING_MAX_CLIENT_LAG_SECONDS_DEFAULT);
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

  /* Enum which represents allowed values of kafka provider. (Hosted Platform) */
  public enum KafkaProvider {
    // Default value, when nothing is provided. (More like Not Applicable)
    UNKNOWN,

    // Kafka/KC is on self hosted node
    SELF_HOSTED,

    // Hosted/managed by Confluent
    CONFLUENT,
    ;

    // All valid enum values
    public static final List<String> PROVIDER_NAMES =
        Arrays.stream(KafkaProvider.values())
            .map(kafkaProvider -> kafkaProvider.name().toLowerCase())
            .collect(Collectors.toList());

    // Returns the KafkaProvider object from string. It does convert an empty or null value to an
    // Enum.
    public static KafkaProvider of(final String kafkaProviderStr) {

      if (Strings.isNullOrEmpty(kafkaProviderStr)) {
        return KafkaProvider.UNKNOWN;
      }

      for (final KafkaProvider b : KafkaProvider.values()) {
        if (b.name().equalsIgnoreCase(kafkaProviderStr)) {
          return b;
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Unsupported provider name: %s. Supported are: %s",
              kafkaProviderStr, String.join(",", PROVIDER_NAMES)));
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
