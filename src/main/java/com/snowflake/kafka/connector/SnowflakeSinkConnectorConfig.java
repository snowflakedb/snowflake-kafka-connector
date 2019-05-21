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

import com.snowflake.kafka.connector.records.SnowflakeAvroConverter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * SnowflakeSinkConnectorConfig class is used for specifying the set of
 * expected configurations.
 * For each configuration, we can specify the name, the type, the default
 * value, the documentation,
 * the group information, the order in the group, the width of the
 * configuration value,
 * and the name suitable for display in the UI.
 * <p>
 * This class is required by Kafka Connect framework.
 * AbstractConfig holds both the original configuration (ConfigDef) that was
 * provided
 * as well as the parsed configuration (Map<String, String>)
 */
public class SnowflakeSinkConnectorConfig extends AbstractConfig
{
  // snowflake kafka connector config

  public static final String OFFSET_FLUSH_INTERVAL_MS = "offset.flush" +
    ".interval.ms";
  public static final long OFFSET_FLUSH_INTERVAL_MS_DEFAULT = 60000;

  public static final String OFFSET_FLUSH_TIMEOUT_MS = "offset.flush.timeout" +
    ".ms";
  public static final long OFFSET_FLUSH_TIMEOUT_MS_DEFAULT = 60000;

  public static final String BUFFER_COUNT_RECORDS = "buffer.count.records";
  public static final long BUFFER_COUNT_RECORDS_DEFAULT = 10000;

  public static final String BUFFER_SIZE_BYTES = "buffer.size.bytes";
  public static final long BUFFER_SIZE_BYTES_DEFAULT = 5000000;

  // snowflake connection and database config

  public static final String SNOWFLAKE_URL = Utils.SF_URL;
  public static final String SNOWFLAKE_URL_DEFAULT = "";

  public static final String SNOWFLAKE_USER = Utils.SF_USER;
  public static final String SNOWFLAKE_USER_DEFAULT = "";

  public static final String SNOWFLAKE_USER_ROLE = Utils.SF_ROLE;
  public static final String SNOWFLAKE_USER_ROLE_DEFAULT = "";

  public static final String SNOWFLAKE_PRIVATE_KEY = Utils.SF_PRIVATE_KEY;
  public static final String SNOWFLAKE_PRIVATE_KEY_DEFAULT = "";

  public static final String SNOWFLAKE_DATABASE = Utils.SF_DATABASE;
  public static final String SNOWFLAKE_DATABASE_DEFAULT = "";

  public static final String SNOWFLAKE_SCHEMA = Utils.SF_SCHEMA;
  public static final String SNOWFLAKE_SCHEMA_DEFAULT = "";

  public static final String TOPICS_TABLES_MAP = "snowflake.topic2table.map";
  public static final String TOPICS_TABLES_MAP_DEFAULT = "";

  public static final String REGISTRY_URL = "value.converter.schema.registry" +
    ".url";
  public static final String REGISTRY_URL_DEFAULT = "";

  public static ConfigDef newConfigDef()
  {
    return new ConfigDef()
      // snowflake avro converter config
      .define(REGISTRY_URL,
        Type.STRING,
        REGISTRY_URL_DEFAULT,
        Importance.LOW,
        "Required by SnowflakeAvroConnector if schema registry is used. " +
          "Leave blank if schema is included in AVRO record")
      // snowflake kafka connector config
      .define(OFFSET_FLUSH_INTERVAL_MS,
        Type.LONG,
        OFFSET_FLUSH_INTERVAL_MS_DEFAULT,
        Importance.LOW,
        "Interval at which to try committing offsets for tasks")
      .define(OFFSET_FLUSH_TIMEOUT_MS,
        Type.LONG,
        OFFSET_FLUSH_TIMEOUT_MS_DEFAULT,
        Importance.LOW,
        "Maximum number of milliseconds to wait for records to flush and" +
          " partition offset data to be committed to offset storage " +
          "before cancelling the process and" +
          " restoring the offset data to be committed in a future " +
          "attempt")
      .define(BUFFER_COUNT_RECORDS,
        Type.LONG,
        BUFFER_COUNT_RECORDS_DEFAULT,
        Importance.LOW,
        "Number of records buffered in memory per partition before " +
          "triggering Snowflake ingestion")
      .define(BUFFER_SIZE_BYTES,
        Type.LONG,
        BUFFER_SIZE_BYTES_DEFAULT,
        Importance.LOW,
        "Cumulative size of records buffered in memory per partition " +
          "before triggering Snowflake ingestion")
      // snowflake connection and database config
      .define(SNOWFLAKE_URL,
        Type.STRING,
        SNOWFLAKE_URL_DEFAULT,
        Importance.HIGH,
        "Snowflake account URL")
      .define(SNOWFLAKE_USER,
        Type.STRING,
        SNOWFLAKE_USER_DEFAULT,
        Importance.HIGH,
        "Snowflake user name")
      .define(SNOWFLAKE_USER_ROLE,
        Type.STRING,
        SNOWFLAKE_USER_ROLE_DEFAULT
        , Importance.HIGH,
        "Snowflake user role")
      .define(SNOWFLAKE_PRIVATE_KEY,
        Type.STRING,
        SNOWFLAKE_PRIVATE_KEY_DEFAULT,
        Importance.HIGH,
        "Private key for Snowflake user")
      .define(SNOWFLAKE_DATABASE,
        Type.STRING,
        SNOWFLAKE_DATABASE_DEFAULT,
        Importance.HIGH,
        "Snowflake database name")
      .define(SNOWFLAKE_SCHEMA,
        Type.STRING,
        SNOWFLAKE_SCHEMA_DEFAULT,
        Importance.HIGH,
        "Snowflake database schema name")
      .define(TOPICS_TABLES_MAP,
        Type.STRING,
        TOPICS_TABLES_MAP_DEFAULT,
        Importance.MEDIUM,
        "Map of topics to tables (optional)." +
          " Format : comma-seperated tuples, e.g. <topic-1>:<table-1>," +
          "<topic-2>:<table-2>,... ");
  }

  final long offset_flush_interval_ms;
  final long offset_flush_timeout_ms;
  final long buffer_count_records;
  final long buffer_size_bytes;

  final String snowflake_url;
  final String snowflake_user_name;
  final String snowflake_user_role;
  final String snowflake_private_key;
  final String snowflake_database;
  final String snowflake_schema;
  final String topics_tables_map;

  public SnowflakeSinkConnectorConfig(ConfigDef config,
                                      Map<String, String> parsedConfig)
  {
    super(config, parsedConfig);

    this.offset_flush_interval_ms = getLong(OFFSET_FLUSH_INTERVAL_MS);
    this.offset_flush_timeout_ms = getLong(OFFSET_FLUSH_TIMEOUT_MS);
    this.buffer_count_records = getLong(BUFFER_COUNT_RECORDS);
    this.buffer_size_bytes = getLong(BUFFER_SIZE_BYTES);

    this.snowflake_url = getString(SNOWFLAKE_URL);
    this.snowflake_user_name = getString(SNOWFLAKE_USER);
    this.snowflake_user_role = getString(SNOWFLAKE_USER_ROLE);
    this.snowflake_private_key = getString(SNOWFLAKE_PRIVATE_KEY);
    this.snowflake_database = getString(SNOWFLAKE_DATABASE);
    this.snowflake_schema = getString(SNOWFLAKE_SCHEMA);
    this.topics_tables_map = getString(TOPICS_TABLES_MAP);
  }

  public SnowflakeSinkConnectorConfig(Map<String, String> parsedConfig)
  {
    this(newConfigDef(), parsedConfig);
  }

  String getSnowflakePrivateKey()
  {
    return getString(SNOWFLAKE_PRIVATE_KEY);
  }   // TODO: why is this needed ?
}
