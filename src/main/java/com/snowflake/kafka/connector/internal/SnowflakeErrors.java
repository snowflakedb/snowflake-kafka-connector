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

package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;

public enum SnowflakeErrors {

  // connector configuration issues 0---
  ERROR_0001(
      "0001",
      "Invalid input connector configuration",
      "input kafka connector configuration is null, missing required values, "
          + "or is invalid. Check logs for list of invalid parameters."),
  ERROR_0002("0002", "Invalid private key", "private key should be a valid PEM RSA private key"),
  ERROR_0003(
      "0003",
      "Missing required parameter",
      "one or multiple required parameters haven't be provided"),
  ERROR_0004("0004", "Empty Stage name", "Input Stage name is empty string or null"),
  ERROR_0005("0005", "Empty Table name", "Input Table name is empty string or null"),
  ERROR_0006("0006", "Empty Pipe name", "Input Pipe name is empty String or null"),
  ERROR_0007(
      "0007",
      "Invalid Snowflake URL",
      "Snowflake URL format: 'https://<account_name>.<region_name>"
          + ".snowflakecomputing.com:443', 'https://' and ':443' are optional."),
  ERROR_0008(
      "0008",
      "Invalid staging file name",
      "File name format: <app_name>/<table_name>/<partition_number"
          + ">/<start_offset>_<end_offset>_<timestamp>.json.gz"),
  ERROR_0009(
      "0009",
      "Invalid value converter",
      "Only support Snowflake Converters (e.g. SnowflakeJsonConverter, "
          + "SnowflakeAvroConverter)"),
  ERROR_0010("0010", "Invalid input record", "Input record value can't be parsed"),
  ERROR_0011("0011", "Failed to load schema from Schema Registry", "Schema ID doesn't exist"),
  ERROR_0012(
      "0012",
      "Failed to connect schema registry service",
      "Schema registry service is not available"),
  ERROR_0013(
      "0013",
      "Missed private key in connector config",
      "private key must be provided with " + Utils.SF_PRIVATE_KEY + " parameter"),
  ERROR_0014(
      "0014",
      "Missed snowflake schema name in connector config",
      "snowflake schema name must be provided with " + Utils.SF_SCHEMA + " " + "parameter"),
  ERROR_0015(
      "0015",
      "Missed snowflake database name in connector config ",
      "snowflake database name must be provided with " + Utils.SF_DATABASE + " " + "parameter"),
  ERROR_0016(
      "0016",
      "Missed snowflake user name in connector config ",
      "snowflake user name must be provided with " + Utils.SF_USER + " parameter"),
  ERROR_0017(
      "0017",
      "Missed snowflake url in connector config ",
      "snowflake URL must be provided with "
          + Utils.SF_URL
          + " parameter, e.g. 'accountname.snoflakecomputing.com'"),
  ERROR_0018(
      "0018",
      "Invalid encrypted private key or passphrase",
      "failed to decrypt private key. Please verify input private key and passphrase. Snowflake"
          + " Kafka Connector only supports encryption algorithms in FIPS 140-2"),
  ERROR_0019(
      "0019",
      "Invalid record data",
      "Unrecognizable record content, please use Snowflake Converters"),
  ERROR_0020("0020", "Invalid topic name", "Topic name is empty String or null"),
  ERROR_0021("0021", "Invalid topic2table map", "Failed to parse topic2table map"),
  ERROR_0022(
      "0022",
      "Invalid proxy host or port",
      "Both host and port need to be provided if one of them is provided"),
  ERROR_0023(
      "0023",
      "Invalid proxy username or password",
      "Both username and password need to be provided if one of them is provided"),
  ERROR_0024(
      "0024",
      "Reader schema invalid",
      "A reader schema is provided but can not be parsed as an Avro schema"),
  ERROR_0025(
      "0025",
      "Duplicate case-insensitive column names detected",
      "Duplicate case-insensitive column names detected. Schematization currently does not support"
          + " this."),
  ERROR_0026(
      "0026",
      "Missed oauth client id in connector config",
      "oauth_client_id must be provided with "
          + Utils.SF_OAUTH_CLIENT_ID
          + " parameter when using oauth as authenticator"),
  ERROR_0027(
      "0027",
      "Missed oauth client secret in connector config",
      "oauth_client_secret must be provided with "
          + Utils.SF_OAUTH_CLIENT_SECRET
          + " parameter when using oauth as authenticator"),
  ERROR_0028(
      "0028",
      "Missed oauth refresh token in connector config",
      "oauth_refresh_token must be provided with "
          + Utils.SF_OAUTH_REFRESH_TOKEN
          + " parameter when using oauth as authenticator"),
  ERROR_0029(
      "0029", "Invalid authenticator", "Authenticator should be either oauth or snowflake_jwt"),
  ERROR_0030(
      "0030",
      String.format(
          "Invalid %s map",
          SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP),
      String.format(
          "Failed to parse %s map",
          SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)),
  ERROR_0031(
      "0031",
      "Failed to combine JDBC properties",
      "One of snowflake.jdbc.map property overrides other jdbc property"),
  ERROR_0032(
      "0032",
      "Iceberg table does not exist or is in invalid format",
      "Check Snowflake Kafka Connector docs for details"),
  // Snowflake connection issues 1---
  ERROR_1001(
      "1001",
      "Failed to connect to Snowflake Server",
      "Snowflake connection issue, reported by Snowflake JDBC"),
  ERROR_1002(
      "1002",
      "Failed to connect to Snowflake Ingestion Service Endpoint",
      "Ingestion Service connection issue, reported by Ingest SDK"),
  ERROR_1003(
      "1003",
      "Snowflake connection is closed",
      "Either the current connection is closed or hasn't connect to snowflake" + " server"),
  ERROR_1004(
      "1004", "Fetching OAuth token fail", "Fail to get OAuth token from authorization server"),
  ERROR_1005(
      "1005",
      "Task failed due to authorization error",
      "Set `enable.task.fail.on.authorization.errors=false` to avoid this behavior"),
  // SQL issues 2---
  ERROR_2001(
      "2001", "Failed to prepare SQL statement", "SQL Exception, reported by Snowflake JDBC"),

  ERROR_2002(
      "2002",
      "Failed to download file",
      "Failed to download file from Snowflake Stage though JDBC"),

  ERROR_2003(
      "2003", "Failed to upload file", "Failed to upload file to Snowflake Stage though JDBC"),
  ERROR_2004(
      "2004",
      "Failed to retrieve list of files",
      "Failed to retrieve list of files on Stage though JDBC"),
  ERROR_2005("2005", "Failed to close connection", "Failed to close snowflake JDBC connection"),
  ERROR_2006(
      "2006", "Failed to connection status", "Failed to retrieve Snowflake JDBC connection Status"),
  ERROR_2007(
      "2007",
      "Failed to create table",
      "Failed to create table on Snowflake, please check that you have permission to do so."),
  ERROR_2008(
      "2008",
      "Failed to create stage",
      "Failed to create stage on Snowflake, please check that you have permission to do so."),
  ERROR_2009(
      "2009",
      "Failed to create pipe",
      "Failed to create pipe on Snowflake, please check that you have permission to do so."),
  ERROR_2010("2010", "Max retry exceeded", "Api retry exceeded the max retry limit"),
  ERROR_2011(
      "2011",
      "Failed to upload file with cache",
      "Failed to upload file to Snowflake Stage though credential caching"),
  ERROR_2012(
      "2012",
      "Failed to append RECORD_METADATA column",
      "Failed to append RECORD_METADATA column due to an existing RECORD_METADATA column with"
          + " non-VARIANT type."),
  ERROR_2013(
      "2013",
      "Failed to append RECORD_METADATA column",
      "Failed to append RECORD_METADATA column, please check that you have permission to do so."),
  ERROR_2014(
      "2014", "Table not exists", "Table not exists. It might have been deleted externally."),
  ERROR_2015(
      "2015", "Failed to append columns", "Failed to append columns during schema evolution"),
  ERROR_2016("2016", "Failed to drop NOT NULL", "Failed to drop NOT NULL during schema evolution"),
  ERROR_2017(
      "2017",
      "Failed to check schema evolution permission",
      "Failed to check schema evolution permission"),

  ERROR_2018(
      "2018",
      "Failed to alter RECORD_METADATA column type for iceberg",
      "Failed to alter RECORD_METADATA column type to required format for iceberg."),
  ERROR_2019(
      "2019",
      "Failed to add RECORD_METADATA column for iceberg",
      "Failed to add RECORD_METADATA column with required format for iceberg."),

  // Snowpipe related issues 3---
  ERROR_3001("3001", "Failed to ingest file", "Exception reported by Ingest SDK"),

  ERROR_3002("3002", "Failed to read load history", "Exception reported by Ingest SDK"),

  ERROR_3003("3003", "Timeout when checking ingest report", "Exceed respond time limitation"),

  ERROR_3004(
      "3004",
      "Snowpipe is not found",
      "Snowpipe object should be created in the connector and registered " + "to pipe list"),
  ERROR_3005(
      "3005",
      "Failed to create pipe",
      "User may have insufficient privileges. If this persists, please "
          + "contact Snowflake support. "),

  // deprecated - ERROR_3006("3006", "Failed to configure client status", "Exception reported by
  // Ingest SDK"),
  // deprecated - ERROR_3007("3007", "Failed to get client status", "Exception reported by Ingest
  // SDK"),
  // deprecated - ERROR_3008("3008", "Failed to ingest file with client info", "Exception reported
  // by Ingest SDK"),

  // Wrong result issues 4---
  ERROR_4001("4001", "Unexpected Result", "Get wrong results from Snowflake service"),
  // Connector internal errors 5---
  ERROR_5001(
      "5001",
      "Failed to create json encoder",
      "Failed to create json encoder in Snowflake Avro Converter"),
  ERROR_5002(
      "5002",
      "Not support data source connector",
      "Snowflake converter doesn't support data source connector yet"),
  ERROR_5003("5003", "Incompatible table", "Table doesn't have a compatible schema"),
  ERROR_5004(
      "5004",
      "Incompatible stage",
      "Stage contains files that were not created by the Snowflake Kafka "
          + "Connector. The stage needs a careful cleanup."),
  ERROR_5005(
      "5005",
      "Incompatible pipe",
      "Pipe exists, but is incompatable with Snowflake Kafka Connector and "
          + "must be dropped before restarting the connector."),
  ERROR_5006(
      "5006",
      "Connector stopped due to invalid configuration",
      "Exception while creating or validating table, stage, or pipe."),
  ERROR_5007(
      "5007",
      "SnowflakeSinkConnector timeout",
      "SnowflakeSinkConnector timed out. Tables or stages are not yet "
          + "available for data ingestion to start. If this persists, please "
          + "contact Snowflake support."),
  ERROR_5008(
      "5008",
      "SnowflakeSinkTask timeout",
      "SnowflakeSinkConnector timed out. Tables or stages are not yet "
          + "available for data ingestion to start. If this persists, please "
          + "contact Snowflake support."),
  ERROR_5009(
      "5009",
      "Partition is not found in task buffer",
      "This should not happen, please contact Snowflake support."),
  ERROR_5010(
      "5010",
      "Connection is null or closed",
      "Connection is closed or null when starting sink service"),
  ERROR_5011(
      "5011", "Data is not broken", "Tried to access broken data but the record is not broken"),
  ERROR_5012("5012", "Data is broken", "Failed to access record data because it is broken"),
  ERROR_5013(
      "5013",
      "Failed to initialize SinkTask",
      "SinkTask hasn't been started before calling OPEN function"),
  ERROR_5014(
      "5014",
      "Failed to put records",
      "SinkTask hasn't been initialized before calling PUT function"),
  ERROR_5015(
      "5015",
      "Invalid SinkRecord received",
      "Error parsing SinkRecord of native converter or SinkRecord header"),
  ERROR_5016(
      "5016",
      "Invalid SinkRecord received",
      "SinkRecord.value and SinkRecord.valueSchema cannot be null unless tombstone record ingestion"
          + " is enabled (see "
          + SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG
          + " for more information."),
  ERROR_5017(
      "5017", "Invalid api call to cached put", "Cached put only support AWS, Azure and GCS."),
  ERROR_5018("5018", "Failed to execute cached put", "Error in cached put command"),
  ERROR_5019("5019", "Failed to get stage storage type", "Error in get storage type"),
  ERROR_5020("5020", "Failed to register MBean in MbeanServer", "Object Name is invalid"),
  ERROR_5021(
      "5021",
      "Failed to get data schema",
      "Failed to get data schema. Unrecognizable data type in JSON object"),
  ERROR_5022("5022", "Invalid column name", "Failed to find column in the schema"),

  ERROR_5023(
      "5023",
      "Failure in Streaming Channel Offset Migration Response",
      "Streaming Channel Offset Migration from Source to Destination Channel has no/invalid"
          + " response, please contact Snowflake Support"),
  ERROR_5024(
      "5024",
      "Timeout while waiting for file cleaner to start",
      "Could not allocate thread for file cleaner to start processing in given time. If problem"
          + " persists, please try setting snowflake.snowpipe.use_new_cleaner to false"),
  ERROR_5025(
      "5025", "Unexpected data type", "Unexpected data type encountered during schema evolution."),
  ERROR_5026(
      "5026",
      "Invalid SinkRecord received",
      "Cannot infer type from null or empty object/list during schema evolution.");

  // properties

  private final String name;
  private final String detail;
  private final String code;

  SnowflakeErrors(String code, String name, String detail) {
    this.code = code;
    this.name = name;
    this.detail = detail;
  }

  public SnowflakeKafkaConnectorException getException() {
    return getException("", null);
  }

  public SnowflakeKafkaConnectorException getException(String msg) {
    return getException(msg, null);
  }

  public SnowflakeKafkaConnectorException getException(Exception e) {
    return getException(e, null);
  }

  public SnowflakeKafkaConnectorException getException(
      Exception e, SnowflakeTelemetryService telemetryService) {
    StringBuilder str = new StringBuilder();
    str.append(e.getMessage());
    for (StackTraceElement element : e.getStackTrace()) {
      str.append("\n").append(element.toString());
    }
    return getException(str.toString(), telemetryService);
  }

  public SnowflakeKafkaConnectorException getException(SnowflakeTelemetryService telemetryService) {
    return getException("", telemetryService);
  }

  /**
   * Convert a given message into SnowflakeKafkaConnectorException.
   *
   * <p>If message is null, we use Enum's toString() method to wrap inside
   * SnowflakeKafkaConnectorException
   *
   * @param msg Message to send to Telemetry Service. Remember, we Strip the message
   * @param telemetryService can be null
   * @return Exception wrapped in Snowflake Connector Exception
   */
  public SnowflakeKafkaConnectorException getException(
      String msg, SnowflakeTelemetryService telemetryService) {
    if (telemetryService != null) {
      telemetryService.reportKafkaConnectFatalError(
          getCode() + msg.substring(0, Math.min(msg.length(), 500)));
    }

    if (msg == null || msg.isEmpty()) {
      return new SnowflakeKafkaConnectorException(toString(), code);
    } else {
      return new SnowflakeKafkaConnectorException(
          Utils.formatLogMessage(
              "Exception: {}\nError Code: {}\nDetail: {}\nMessage: {}", name, code, detail, msg),
          code);
    }
  }

  public String getCode() {
    return code;
  }

  public String getDetail() {
    return this.detail;
  }

  @Override
  public String toString() {
    return Utils.formatLogMessage("Exception: {}\nError Code: {}\nDetail: {}", name, code, detail);
  }
}
