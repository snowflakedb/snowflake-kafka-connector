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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
/**
 * SnowflakeSinkConnectorConfig class is used for specifying the set of
 * expected configurations.
 * For each configuration, we can specify the name, the type, the default
 * value, the documentation,
 * the group information, the order in the group, the width of the
 * configuration value,
 * and the name suitable for display in the UI.
 */
public class SnowflakeSinkConnectorConfig
{

  static final String NAME = Utils.NAME;
  static final String TOPICS = "topics";

  // Connector config
  private static final String CONNECTOR_CONFIG = "Connector Config";
  static final String BUFFER_COUNT_RECORDS = "buffer.count.records";
  public static final long BUFFER_COUNT_RECORDS_DEFAULT = 10000;
  static final String BUFFER_SIZE_BYTES = "buffer.size.bytes";
  public static final long BUFFER_SIZE_BYTES_DEFAULT = 5000000;
  public static final long BUFFER_SIZE_BYTES_MAX = 100000000;
  static final String TOPICS_TABLES_MAP = "snowflake.topic2table.map";

  // Time in seconds
  public static final long BUFFER_FLUSH_TIME_SEC_MIN = 10;
  public static final long BUFFER_FLUSH_TIME_SEC_DEFAULT = 30;
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
  static final String JVM_PROXY_HOST = "jvm.proxy.host";
  static final String JVM_PROXY_PORT = "jvm.proxy.port";

  // Schema Registry Info
  private static final String SCHEMA_REGISTRY_INFO = "Schema Registry Info";
  static final String SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE =
    "value.converter.basic.auth.credentials.source";
  static final String SCHEMA_REGISTRY_AUTH_USER_INFO =
    "value.converter.basic.auth.user.info";
  private static final String REGISTRY_URL = "value.converter.schema.registry.url";

  // Snowflake Metadata Flags
  private static final String SNOWFLAKE_METADATA_FLAGS = "Snowflake Metadata Flags";
  public static final String SNOWFLAKE_METADATA_CREATETIME = "snowflake.metadata.createtime";
  public static final String SNOWFLAKE_METADATA_TOPIC = "snowflake.metadata.topic";
  public static final String SNOWFLAKE_METADATA_OFFSET_AND_PARTITION =
    "snowflake.metadata.offset.and.partition";
  public static final String SNOWFLAKE_METADATA_ALL = "snowflake.metadata.all";
  public static final String SNOWFLAKE_METADATA_DEFAULT = "true";


  static ConfigDef newConfigDef()
  {
    return new ConfigDef()
      //snowflake login info
      .define(SNOWFLAKE_URL,
        Type.STRING,
        "",
        Importance.HIGH,
        "Snowflake account url",
        SNOWFLAKE_LOGIN_INFO,
        0,
        ConfigDef.Width.NONE,
        SNOWFLAKE_URL)
      .define(SNOWFLAKE_USER,
        Type.STRING,
        "",
        Importance.HIGH,
        "Snowflake user name",
        SNOWFLAKE_LOGIN_INFO,
        1,
        ConfigDef.Width.NONE,
        SNOWFLAKE_USER)
      .define(SNOWFLAKE_PRIVATE_KEY,
        Type.STRING,
        "",
        Importance.HIGH,
        "Private key for Snowflake user",
        SNOWFLAKE_LOGIN_INFO,
        2,
        ConfigDef.Width.NONE,
        SNOWFLAKE_PRIVATE_KEY)
      .define(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
        Type.STRING,
        "",
        Importance.LOW,
        "Passphrase of private key if encrypted",
        SNOWFLAKE_LOGIN_INFO,
        3,
        ConfigDef.Width.NONE,
        SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
      .define(SNOWFLAKE_DATABASE,
        Type.STRING,
        "",
        Importance.HIGH,
        "Snowflake database name",
        SNOWFLAKE_LOGIN_INFO,
        4,
        ConfigDef.Width.NONE,
        SNOWFLAKE_DATABASE)
      .define(SNOWFLAKE_SCHEMA,
        Type.STRING,
        "",
        Importance.HIGH,
        "Snowflake database schema name",
        SNOWFLAKE_LOGIN_INFO,
        5,
        ConfigDef.Width.NONE,
        SNOWFLAKE_SCHEMA)
      //proxy
      .define(JVM_PROXY_HOST,
        Type.STRING,
        "",
        Importance.LOW,
        "JVM option: https.proxyHost",
        PROXY_INFO,
        0,
        ConfigDef.Width.NONE,
        JVM_PROXY_HOST
        )
      .define(JVM_PROXY_PORT,
        Type.STRING,
        "",
        Importance.LOW,
        "JVM option: https.proxyPort",
        PROXY_INFO,
        1,
        ConfigDef.Width.NONE,
        JVM_PROXY_PORT)
      //schema registry
      .define(REGISTRY_URL,
        Type.STRING,
        "",
        Importance.LOW,
        "Required by SnowflakeAvroConnector if schema registry is used. Leave blank if schema is included in AVRO record",
        SCHEMA_REGISTRY_INFO,
        0,
        ConfigDef.Width.NONE,
        REGISTRY_URL)
      .define(SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE,
        Type.STRING,
        "",
        Importance.LOW,
        "Required by SnowflakeAvroConnector if schema registry authentication used. e.g USER_INFO",
        SCHEMA_REGISTRY_INFO,
        1,
        ConfigDef.Width.NONE,
        SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE)
      .define(SCHEMA_REGISTRY_AUTH_USER_INFO,
        Type.STRING,
        "",
        Importance.LOW,
        "User info of schema registry authentication, format: <user name>:<password>",
        SCHEMA_REGISTRY_INFO,
        2,
        ConfigDef.Width.NONE,
        SCHEMA_REGISTRY_AUTH_USER_INFO
        )
      //Connector Config
      .define(TOPICS_TABLES_MAP,
        Type.STRING,
        "",
        Importance.LOW,
        "Map of topics to tables (optional). Format : comma-seperated tuples, e.g. <topic-1>:<table-1>,<topic-2>:<table-2>,... ",
        CONNECTOR_CONFIG,
        0,
        ConfigDef.Width.NONE,
        TOPICS_TABLES_MAP)
      .define(BUFFER_COUNT_RECORDS,
        Type.LONG,
        BUFFER_COUNT_RECORDS_DEFAULT,
        Importance.LOW,
        "Number of records buffered in memory per partition before triggering Snowflake ingestion",
        CONNECTOR_CONFIG,
        1,
        ConfigDef.Width.NONE,
        BUFFER_COUNT_RECORDS)
      .define(BUFFER_SIZE_BYTES,
        Type.LONG,
        BUFFER_SIZE_BYTES_DEFAULT,
        Importance.LOW,
        "Cumulative size of records buffered in memory per partition before triggering Snowflake ingestion",
        CONNECTOR_CONFIG,
        2,
        ConfigDef.Width.NONE,
        BUFFER_SIZE_BYTES)
      .define(BUFFER_FLUSH_TIME_SEC,
        Type.LONG,
        BUFFER_FLUSH_TIME_SEC_DEFAULT,
        Importance.LOW,
        "The time in seconds to flush cached data",
        CONNECTOR_CONFIG,
        3,
        ConfigDef.Width.NONE,
        BUFFER_FLUSH_TIME_SEC)
      .define(SNOWFLAKE_METADATA_ALL,
        Type.STRING,
        SNOWFLAKE_METADATA_DEFAULT,
        Importance.LOW,
        "Flag to control whether there is metadata collected. If set to false, all metadata will be dropped",
        SNOWFLAKE_METADATA_FLAGS,
        0,
        ConfigDef.Width.NONE,
        SNOWFLAKE_METADATA_ALL)
      .define(SNOWFLAKE_METADATA_CREATETIME,
        Type.STRING,
        SNOWFLAKE_METADATA_DEFAULT,
        Importance.LOW,
        "Flag to control whether createtime is collected in snowflake metadata",
        SNOWFLAKE_METADATA_FLAGS,
        1,
        ConfigDef.Width.NONE,
        SNOWFLAKE_METADATA_CREATETIME)
      .define(SNOWFLAKE_METADATA_TOPIC,
        Type.STRING,
        SNOWFLAKE_METADATA_DEFAULT,
        Importance.LOW,
        "Flag to control whether kafka topic name is collected in snowflake metadata",
        SNOWFLAKE_METADATA_FLAGS,
        2,
        ConfigDef.Width.NONE,
        SNOWFLAKE_METADATA_TOPIC)
      .define(SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
        Type.STRING,
        SNOWFLAKE_METADATA_DEFAULT,
        Importance.LOW,
        "Flag to control whether kafka partition and offset are collected in snowflake metadata",
        SNOWFLAKE_METADATA_FLAGS,
        3,
        ConfigDef.Width.NONE,
        SNOWFLAKE_METADATA_OFFSET_AND_PARTITION)
      ;
  }
}
