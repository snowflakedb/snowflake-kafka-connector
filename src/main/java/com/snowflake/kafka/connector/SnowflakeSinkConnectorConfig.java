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

  static final String NAME = "name";
  static final String TOPICS = "topics";

  // snowflake kafka connector config
  static final String BUFFER_COUNT_RECORDS = "buffer.count.records";
  static final long BUFFER_COUNT_RECORDS_DEFAULT = 10000;

  static final String BUFFER_SIZE_BYTES = "buffer.size.bytes";
  static final long BUFFER_SIZE_BYTES_DEFAULT = 5000000;

  // snowflake connection and database config

  static final String SNOWFLAKE_URL = Utils.SF_URL;
  static final String SNOWFLAKE_URL_DEFAULT = "";

  static final String SNOWFLAKE_USER = Utils.SF_USER;
  static final String SNOWFLAKE_USER_DEFAULT = "";

  static final String SNOWFLAKE_PRIVATE_KEY = Utils.SF_PRIVATE_KEY;
  static final String SNOWFLAKE_PRIVATE_KEY_DEFAULT = "";

  static final String SNOWFLAKE_DATABASE = Utils.SF_DATABASE;
  static final String SNOWFLAKE_DATABASE_DEFAULT = "";

  static final String SNOWFLAKE_SCHEMA = Utils.SF_SCHEMA;
  static final String SNOWFLAKE_SCHEMA_DEFAULT = "";

  static final String TOPICS_TABLES_MAP = "snowflake.topic2table.map";
  static final String TOPICS_TABLES_MAP_DEFAULT = "";

  static final String REGISTRY_URL = "value.converter.schema.registry.url";
  static final String REGISTRY_URL_DEFAULT = "";

  //jvm proxy
  static final String JVM_PROXY_HOST = "jvm.proxy.host";
  static final String JVM_PROXY_HOST_DEFAULT = "";
  
  static final String JVM_PROXY_PORT = "jvm.proxy.port";
  static final String JVM_PROXY_PORT_DEFAULT = "";
  

  static ConfigDef newConfigDef()
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
                "<topic-2>:<table-2>,... ")
        .define(JVM_PROXY_HOST,
            Type.STRING,
            JVM_PROXY_HOST_DEFAULT,
            Importance.LOW,
            "jvm option: https.proxyHost")
        .define(JVM_PROXY_PORT,
            Type.STRING,
            JVM_PROXY_PORT_DEFAULT,
            Importance.LOW,
            "jvm option: https.proxyPort")
        ;
  }

}
