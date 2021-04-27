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

import com.snowflake.kafka.connector.internal.Logging;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SnowflakeSinkConnectorConfig class is used for specifying the set of expected configurations. For
 * each configuration, we can specify the name, the type, the default value, the documentation, the
 * group information, the order in the group, the width of the configuration value, and the name
 * suitable for display in the UI.
 */
public class SnowflakeSinkConnectorConfig {

  static final String NAME = Utils.NAME;
  static final String TOPICS = "topics";

  // Connector config
  private static final String CONNECTOR_CONFIG = "Connector Config";
  static final String BUFFER_COUNT_RECORDS = "buffer.count.records";
  public static final long BUFFER_COUNT_RECORDS_DEFAULT = 10000;
  static final String BUFFER_SIZE_BYTES = "buffer.size.bytes";
  public static final long BUFFER_SIZE_BYTES_DEFAULT = 5000000;
  public static final long BUFFER_SIZE_BYTES_MIN = 1;
  static final String TOPICS_TABLES_MAP = "snowflake.topic2table.map";

  // Time in seconds
  public static final long BUFFER_FLUSH_TIME_SEC_MIN = 10;
  public static final long BUFFER_FLUSH_TIME_SEC_DEFAULT = 120;
  static final String BUFFER_FLUSH_TIME_SEC = "buffer.flush.time";

  // Snowflake connection and database config
  private static final String SNOWFLAKE_LOGIN_INFO = "Snowflake Login Info";
  static final String SNOWFLAKE_URL = Utils.SF_URL;
  static final String SNOWFLAKE_USER = Utils.SF_USER;
  static final String SNOWFLAKE_PRIVATE_KEY = Utils.SF_PRIVATE_KEY;
  static final String SNOWFLAKE_DATABASE = Utils.SF_DATABASE;
  static final String SNOWFLAKE_SCHEMA = Utils.SF_SCHEMA;
  static final String SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = Utils.PRIVATE_KEY_PASSPHRASE;

  // Proxy Info
  private static final String PROXY_INFO = "Proxy Info";
  public static final String JVM_PROXY_HOST = "jvm.proxy.host";
  public static final String JVM_PROXY_PORT = "jvm.proxy.port";
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

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SnowflakeSinkConnectorConfig.class.getName());

  private static final ConfigDef.Validator nonEmptyStringValidator = new ConfigDef.NonEmptyString();
  private static final ConfigDef.Validator topicToTableValidator = new TopicToTableValidator();

  static void setDefaultValues(Map<String, String> config) {
    setFieldToDefaultValues(config, BUFFER_COUNT_RECORDS, BUFFER_COUNT_RECORDS_DEFAULT);

    setFieldToDefaultValues(config, BUFFER_SIZE_BYTES, BUFFER_SIZE_BYTES_DEFAULT);

    setFieldToDefaultValues(config, BUFFER_FLUSH_TIME_SEC, BUFFER_FLUSH_TIME_SEC_DEFAULT);
  }

  static void setFieldToDefaultValues(Map<String, String> config, String field, Long value) {
    if (!config.containsKey(field)) {
      config.put(field, value + "");
      LOGGER.info(Logging.logMessage("{} set to default {} seconds", field, value));
    }
  }

  /**
   * Get a property from the config map
   *
   * @param config connector configuration
   * @param key name of the key to be retrived
   * @return proprity value or null
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
            JVM_PROXY_USERNAME,
            Type.STRING,
            "",
            Importance.LOW,
            "JVM proxy username",
            PROXY_INFO,
            2,
            ConfigDef.Width.NONE,
            JVM_PROXY_USERNAME)
        .define(
            JVM_PROXY_PASSWORD,
            Type.STRING,
            "",
            Importance.LOW,
            "JVM proxy password",
            PROXY_INFO,
            3,
            ConfigDef.Width.NONE,
            JVM_PROXY_PASSWORD)
        // Connector Config
        .define(
            TOPICS_TABLES_MAP,
            Type.STRING,
            "",
            topicToTableValidator,
            Importance.LOW,
            "Map of topics to tables (optional). Format : comma-seperated tuples, e.g."
                + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ",
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
            ConfigDef.Range.atLeast(BUFFER_FLUSH_TIME_SEC_MIN),
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
            SNOWFLAKE_METADATA_OFFSET_AND_PARTITION);
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
      return "Topic to table map format : comma-seperated tuples, e.g."
          + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ";
    }
  }
}
