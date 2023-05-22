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
import com.google.common.collect.ImmutableSet;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

/**
 * SnowflakeSinkConnectorConfig class is used for specifying the set of expected configurations. For
 * each configuration, we can specify the name, the type, the default value, the documentation, the
 * group information, the order in the group, the width of the configuration value, and the name
 * suitable for display in the UI.
 */
public class SnowflakeSinkConnectorConfig {

  static final String NAME = Utils.NAME;
  public static final String TOPICS = "topics";

  // Connector config
  private static final String CONNECTOR_CONFIG = "Connector Config";
  public static final String BUFFER_COUNT_RECORDS = "buffer.count.records";
  public static final long BUFFER_COUNT_RECORDS_DEFAULT = 10000;
  public static final String BUFFER_SIZE_BYTES = "buffer.size.bytes";
  public static final long BUFFER_SIZE_BYTES_DEFAULT = 5000000;
  public static final long BUFFER_SIZE_BYTES_MIN = 1;
  static final String TOPICS_TABLES_MAP = "snowflake.topic2table.map";

  // For tombstone records
  public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";

  // Time in seconds
  public static final long BUFFER_FLUSH_TIME_SEC_MIN = 10;
  public static final long BUFFER_FLUSH_TIME_SEC_DEFAULT = 120;
  public static final String BUFFER_FLUSH_TIME_SEC = "buffer.flush.time";

  // Snowflake connection and database config
  private static final String SNOWFLAKE_LOGIN_INFO = "Snowflake Login Info";
  static final String SNOWFLAKE_URL = Utils.SF_URL;
  static final String SNOWFLAKE_USER = Utils.SF_USER;
  static final String SNOWFLAKE_PRIVATE_KEY = Utils.SF_PRIVATE_KEY;
  static final String SNOWFLAKE_DATABASE = Utils.SF_DATABASE;
  static final String SNOWFLAKE_SCHEMA = Utils.SF_SCHEMA;
  static final String SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = Utils.PRIVATE_KEY_PASSPHRASE;

  // For Snowpipe Streaming client
  public static final String SNOWFLAKE_ROLE = Utils.SF_ROLE;
  public static final String ENABLE_SCHEMATIZATION_CONFIG = "snowflake.enable.schematization";
  public static final String ENABLE_SCHEMATIZATION_DEFAULT = "false";

  public static final String SCHEMATIZATION_AUTO_CONFIG = "snowflake.schematization.auto";
  public static final String SCHEMATIZATION_AUTO_DEFAULT = "true";
  public static final String SCHEMATIZATION_AUTO_DISPLAY = "Use automatic schema evolution";
  public static final String SCHEMATIZATION_AUTO_DOC =
      "If true, use snowflake automatic schema evolution feature."
          + "NOTE: you need to grant evolve schema to " + SNOWFLAKE_USER;
  
  // Proxy Info
  private static final String PROXY_INFO = "Proxy Info";
  public static final String JVM_PROXY_HOST = "jvm.proxy.host";
  public static final String JVM_PROXY_PORT = "jvm.proxy.port";
  public static final String JVM_NON_PROXY_HOSTS = "jvm.nonProxy.hosts";
  public static final String JVM_PROXY_USERNAME = "jvm.proxy.username";
  public static final String JVM_PROXY_PASSWORD = "jvm.proxy.password";

  // JDBC logging directory Info (environment variable)
  static final String SNOWFLAKE_JDBC_LOG_DIR = "JDBC_LOG_DIR";

  // JDBC trace Info (environment variable)
  public static final String SNOWFLAKE_JDBC_TRACE = "JDBC_TRACE";

  // Snowflake Metadata Flags
  private static final String SNOWFLAKE_METADATA_FLAGS = "Snowflake Metadata Flags";
  public static final String SNOWFLAKE_METADATA_CREATETIME = "snowflake.metadata.createtime";
  public static final String SNOWFLAKE_METADATA_TOPIC = "snowflake.metadata.topic";
  public static final String SNOWFLAKE_METADATA_OFFSET_AND_PARTITION =
      "snowflake.metadata.offset.and.partition";
  public static final String SNOWFLAKE_METADATA_ALL = "snowflake.metadata.all";
  public static final String SNOWFLAKE_METADATA_DEFAULT = "true";

  // Where is Kafka hosted? self, confluent or any other in future.
  // By default it will be None since this is not enforced and only used for monitoring
  public static final String PROVIDER_CONFIG = "provider";

  // metrics
  public static final String JMX_OPT = "jmx";
  public static final boolean JMX_OPT_DEFAULT = true;

  // for Snowpipe vs Streaming Snowpipe
  public static final String INGESTION_METHOD_OPT = "snowflake.ingestion.method";
  public static final String INGESTION_METHOD_DEFAULT_SNOWPIPE =
      IngestionMethodConfig.SNOWPIPE.toString();

  // This is the streaming bdec file version which can be defined in config
  // NOTE: Please do not override this value unless recommended from snowflake
  public static final String SNOWPIPE_STREAMING_FILE_VERSION = "snowflake.streaming.file.version";

  // TESTING
  public static final String REBALANCING = "snowflake.test.rebalancing";
  public static final boolean REBALANCING_DEFAULT = false;

  private static final KCLogger LOGGER = new KCLogger(SnowflakeSinkConnectorConfig.class.getName());

  private static final ConfigDef.Validator nonEmptyStringValidator = new ConfigDef.NonEmptyString();
  private static final ConfigDef.Validator topicToTableValidator = new TopicToTableValidator();
  private static final ConfigDef.Validator KAFKA_PROVIDER_VALIDATOR = new KafkaProviderValidator();

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

  public static final String ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG =
      "enable.streaming.client.optimization";
  public static final String ENABLE_STREAMING_CLIENT_OPTIMIZATION_DISPLAY =
      "Enable streaming client optimization";
  public static final boolean ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT = false;
  public static final String ENABLE_STREAMING_CLIENT_OPTIMIZATION_DOC =
      "Whether to optimize the streaming client to reduce cost. Note that this may affect"
          + " throughput or latency and can only be set if Streaming Snowpipe is enabled";

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
      ImmutableSet.of(
          "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
          "com.snowflake.kafka.connector.records.SnowflakeAvroConverterWithoutSchemaRegistry",
          "com.snowflake.kafka.connector.records.SnowflakeAvroConverter");

  public static void setDefaultValues(Map<String, String> config) {
    setFieldToDefaultValues(config, BUFFER_COUNT_RECORDS, BUFFER_COUNT_RECORDS_DEFAULT);

    setFieldToDefaultValues(config, BUFFER_SIZE_BYTES, BUFFER_SIZE_BYTES_DEFAULT);

    setFieldToDefaultValues(config, BUFFER_FLUSH_TIME_SEC, BUFFER_FLUSH_TIME_SEC_DEFAULT);
  }

  static void setFieldToDefaultValues(Map<String, String> config, String field, Long value) {
    if (!config.containsKey(field)) {
      config.put(field, value + "");
      LOGGER.info("{} set to default {} seconds", field, value);
    }
  }

  /**
   * Get a property from the config map
   *
   * @param config connector configuration
   * @param key name of the key to be retrieved
   * @return property value or null
   */
  static String getProperty(final Map<String, String> config, final String key) {
    if (config.containsKey(key) && !config.get(key).isEmpty()) {
      return config.get(key);
    } else {
      return null;
    }
  }

  static ConfigDef newConfigDef() {
    return new ConfigDef()
        // snowflake login info
        .define(
            SNOWFLAKE_URL,
            Type.STRING,
            null,
            nonEmptyStringValidator,
            Importance.HIGH,
            "Snowflake account url",
            SNOWFLAKE_LOGIN_INFO,
            0,
            ConfigDef.Width.NONE,
            SNOWFLAKE_URL)
        .define(
            SNOWFLAKE_USER,
            Type.STRING,
            null,
            nonEmptyStringValidator,
            Importance.HIGH,
            "Snowflake user name",
            SNOWFLAKE_LOGIN_INFO,
            1,
            ConfigDef.Width.NONE,
            SNOWFLAKE_USER)
        .define(
            SNOWFLAKE_PRIVATE_KEY,
            Type.PASSWORD,
            "",
            Importance.HIGH,
            "Private key for Snowflake user",
            SNOWFLAKE_LOGIN_INFO,
            2,
            ConfigDef.Width.NONE,
            SNOWFLAKE_PRIVATE_KEY)
        .define(
            SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
            Type.PASSWORD,
            "",
            Importance.LOW,
            "Passphrase of private key if encrypted",
            SNOWFLAKE_LOGIN_INFO,
            3,
            ConfigDef.Width.NONE,
            SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
        .define(
            SNOWFLAKE_DATABASE,
            Type.STRING,
            null,
            nonEmptyStringValidator,
            Importance.HIGH,
            "Snowflake database name",
            SNOWFLAKE_LOGIN_INFO,
            4,
            ConfigDef.Width.NONE,
            SNOWFLAKE_DATABASE)
        .define(
            SNOWFLAKE_SCHEMA,
            Type.STRING,
            null,
            nonEmptyStringValidator,
            Importance.HIGH,
            "Snowflake database schema name",
            SNOWFLAKE_LOGIN_INFO,
            5,
            ConfigDef.Width.NONE,
            SNOWFLAKE_SCHEMA)
        .define(
            SNOWFLAKE_ROLE,
            Type.STRING,
            null,
            nonEmptyStringValidator,
            Importance.LOW,
            "Snowflake role: snowflake.role.name",
            SNOWFLAKE_LOGIN_INFO,
            6,
            ConfigDef.Width.NONE,
            SNOWFLAKE_ROLE)
        // proxy
        .define(
            JVM_PROXY_HOST,
            Type.STRING,
            "",
            Importance.LOW,
            "JVM option: https.proxyHost",
            PROXY_INFO,
            0,
            ConfigDef.Width.NONE,
            JVM_PROXY_HOST)
        .define(
            JVM_PROXY_PORT,
            Type.STRING,
            "",
            Importance.LOW,
            "JVM option: https.proxyPort",
            PROXY_INFO,
            1,
            ConfigDef.Width.NONE,
            JVM_PROXY_PORT)
        .define(
            JVM_NON_PROXY_HOSTS,
            Type.STRING,
            "",
            Importance.LOW,
            "JVM option: http.nonProxyHosts",
            PROXY_INFO,
            2,
            ConfigDef.Width.NONE,
            JVM_NON_PROXY_HOSTS)
        .define(
            JVM_PROXY_USERNAME,
            Type.STRING,
            "",
            Importance.LOW,
            "JVM proxy username",
            PROXY_INFO,
            3,
            ConfigDef.Width.NONE,
            JVM_PROXY_USERNAME)
        .define(
            JVM_PROXY_PASSWORD,
            Type.STRING,
            "",
            Importance.LOW,
            "JVM proxy password",
            PROXY_INFO,
            4,
            ConfigDef.Width.NONE,
            JVM_PROXY_PASSWORD)
        // Connector Config
        .define(
            TOPICS_TABLES_MAP,
            Type.STRING,
            "",
            topicToTableValidator,
            Importance.LOW,
            "Map of topics to tables (optional). Format : comma-separated tuples, e.g."
                + " <topic-1>:<table-1>,<topic-2>:<table-2>,... \n"
                + "Generic regex matching is possible using the following syntax:"
                + Utils.TOPIC_MATCHER_PREFIX + "^[^.]\\w+.\\w+.(.*):$1\n"
                + "NOTE: topics names cannot contain \":\" or \",\" so the regex should not contain these characters either\n",
            CONNECTOR_CONFIG,
            0,
            ConfigDef.Width.NONE,
            TOPICS_TABLES_MAP)
        .define(
            BUFFER_COUNT_RECORDS,
            Type.LONG,
            BUFFER_COUNT_RECORDS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            Importance.LOW,
            "Number of records buffered in memory per partition before triggering Snowflake"
                + " ingestion",
            CONNECTOR_CONFIG,
            1,
            ConfigDef.Width.NONE,
            BUFFER_COUNT_RECORDS)
        .define(
            BUFFER_SIZE_BYTES,
            Type.LONG,
            BUFFER_SIZE_BYTES_DEFAULT,
            ConfigDef.Range.atLeast(1),
            Importance.LOW,
            "Cumulative size of records buffered in memory per partition before triggering"
                + " Snowflake ingestion",
            CONNECTOR_CONFIG,
            2,
            ConfigDef.Width.NONE,
            BUFFER_SIZE_BYTES)
        .define(
            BUFFER_FLUSH_TIME_SEC,
            Type.LONG,
            BUFFER_FLUSH_TIME_SEC_DEFAULT,
            ConfigDef.Range.atLeast(StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC),
            Importance.LOW,
            "The time in seconds to flush cached data",
            CONNECTOR_CONFIG,
            3,
            ConfigDef.Width.NONE,
            BUFFER_FLUSH_TIME_SEC)
        .define(
            SNOWFLAKE_METADATA_ALL,
            Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            Importance.LOW,
            "Flag to control whether there is metadata collected. If set to false, all metadata"
                + " will be dropped",
            SNOWFLAKE_METADATA_FLAGS,
            0,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_ALL)
        .define(
            SNOWFLAKE_METADATA_CREATETIME,
            Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            Importance.LOW,
            "Flag to control whether createtime is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS,
            1,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_CREATETIME)
        .define(
            SNOWFLAKE_METADATA_TOPIC,
            Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            Importance.LOW,
            "Flag to control whether kafka topic name is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS,
            2,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_TOPIC)
        .define(
            SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
            Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            Importance.LOW,
            "Flag to control whether kafka partition and offset are collected in snowflake"
                + " metadata",
            SNOWFLAKE_METADATA_FLAGS,
            3,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_OFFSET_AND_PARTITION)
        .define(
            PROVIDER_CONFIG,
            Type.STRING,
            KafkaProvider.UNKNOWN.name(),
            KAFKA_PROVIDER_VALIDATOR,
            Importance.LOW,
            "Whether kafka is running on Confluent code, self hosted or other managed service")
        .define(
            BEHAVIOR_ON_NULL_VALUES_CONFIG,
            Type.STRING,
            BehaviorOnNullValues.DEFAULT.toString(),
            BehaviorOnNullValues.VALIDATOR,
            Importance.LOW,
            "How to handle records with a null value (i.e. Kafka tombstone records)."
                + " Valid options are 'DEFAULT' and 'IGNORE'.",
            CONNECTOR_CONFIG,
            4,
            ConfigDef.Width.NONE,
            BEHAVIOR_ON_NULL_VALUES_CONFIG)
        .define(
            JMX_OPT,
            ConfigDef.Type.BOOLEAN,
            JMX_OPT_DEFAULT,
            ConfigDef.Importance.HIGH,
            "Whether to enable JMX MBeans for custom SF metrics")
        .define(
            REBALANCING,
            Type.BOOLEAN,
            REBALANCING_DEFAULT,
            Importance.LOW,
            "Whether to trigger a rebalancing by exceeding the max poll interval (Used only in"
                + " testing)")
        .define(
            INGESTION_METHOD_OPT,
            Type.STRING,
            INGESTION_METHOD_DEFAULT_SNOWPIPE,
            IngestionMethodConfig.VALIDATOR,
            Importance.LOW,
            "Acceptable values for Ingestion: SNOWPIPE or Streaming ingest respectively",
            CONNECTOR_CONFIG,
            5,
            ConfigDef.Width.NONE,
            INGESTION_METHOD_OPT)
        .define(
            SNOWPIPE_STREAMING_FILE_VERSION,
            Type.STRING,
            "", // default is handled in Ingest SDK
            null, // no validator
            Importance.LOW,
            "Acceptable values for Snowpipe Streaming BDEC Versions: 1 and 3. Check Ingest"
                + " SDK for default behavior. Please do not set this unless Absolutely needed. ",
            CONNECTOR_CONFIG,
            6,
            ConfigDef.Width.NONE,
            SNOWPIPE_STREAMING_FILE_VERSION)
        .define(
            ERRORS_TOLERANCE_CONFIG,
            Type.STRING,
            ERRORS_TOLERANCE_DEFAULT,
            ErrorTolerance.VALIDATOR,
            Importance.LOW,
            ERRORS_TOLERANCE_DOC,
            ERROR_GROUP,
            0,
            ConfigDef.Width.NONE,
            ERRORS_TOLERANCE_DISPLAY)
        .define(
            ERRORS_LOG_ENABLE_CONFIG,
            Type.BOOLEAN,
            ERRORS_LOG_ENABLE_DEFAULT,
            Importance.LOW,
            ERRORS_LOG_ENABLE_DOC,
            ERROR_GROUP,
            1,
            ConfigDef.Width.NONE,
            ERRORS_LOG_ENABLE_DISPLAY)
        .define(
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
            Type.STRING,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT,
            Importance.LOW,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DOC,
            ERROR_GROUP,
            2,
            ConfigDef.Width.NONE,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DISPLAY)
        .define(
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            Type.BOOLEAN,
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT,
            Importance.LOW,
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_DOC,
            CONNECTOR_CONFIG,
            7,
            ConfigDef.Width.NONE,
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_DISPLAY)
        .define(
            ENABLE_MDC_LOGGING_CONFIG,
            Type.BOOLEAN,
            ENABLE_MDC_LOGGING_DEFAULT,
            Importance.LOW,
            ENABLE_MDC_LOGGING_DOC,
            CONNECTOR_CONFIG,
            8,
            ConfigDef.Width.NONE,
            ENABLE_MDC_LOGGING_DISPLAY);
  }

  public static class TopicToTableValidator implements ConfigDef.Validator {
    public TopicToTableValidator() {}

    public void ensureValid(String name, Object value) {
      String s = (String) value;
      if (s != null && !s.isEmpty()) // this value is optional and can be empty
      {
        if (Utils.parseTopicToTableMap(s) == null) {
          throw new ConfigException(
              name, value, "Format: <topic-1>:<table-1>,<topic-2>:<table-2>,...");
        }
      }
    }

    public String toString() {
      return "Topic to table map format : comma-separated tuples, e.g."
          + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ";
    }
  }

  /* Validator to validate Kafka Provider values which says where kafka is hosted */
  public static class KafkaProviderValidator implements ConfigDef.Validator {
    public KafkaProviderValidator() {}

    // This API is called by framework to ensure the validity when connector is started or when a
    // validate REST API is called
    @Override
    public void ensureValid(String name, Object value) {
      assert value instanceof String;
      final String strValue = (String) value;
      // The value can be null or empty.
      try {
        KafkaProvider kafkaProvider = KafkaProvider.of(strValue);
      } catch (final IllegalArgumentException e) {
        throw new ConfigException(PROVIDER_CONFIG, value, e.getMessage());
      }
    }

    public String toString() {
      return "Whether kafka is running on Confluent code, self hosted or other managed service."
          + " Allowed values are:"
          + String.join(",", KafkaProvider.PROVIDER_NAMES);
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
}
