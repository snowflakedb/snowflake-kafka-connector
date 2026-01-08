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

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
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
      "private key must be provided with "
          + KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY
          + " parameter"),
  ERROR_0014(
      "0014",
      "Missed snowflake schema name in connector config",
      "snowflake schema name must be provided with "
          + KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME
          + " "
          + "parameter"),
  ERROR_0015(
      "0015",
      "Missed snowflake database name in connector config ",
      "snowflake database name must be provided with "
          + KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME
          + " "
          + "parameter"),
  ERROR_0016(
      "0016",
      "Missed snowflake user name in connector config ",
      "snowflake user name must be provided with "
          + KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME
          + " parameter"),
  ERROR_0017(
      "0017",
      "Missed snowflake url in connector config ",
      "snowflake URL must be provided with "
          + KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME
          + " parameter, e.g. 'accountname.snoflakecomputing.com'"),
  ERROR_0018(
      "0018",
      "Invalid encrypted private key or passphrase",
      "failed to decrypt private key. Please verify input private key and passphrase. Snowflake"
          + " Kafka Connector only supports encryption algorithms in FIPS 140-2"),
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
  ERROR_0030(
      "0030",
      String.format(
          "Invalid %s map",
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP),
      String.format(
          "Failed to parse %s map",
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)),
  ERROR_0031(
      "0031",
      "Failed to combine JDBC properties",
      "One of snowflake.jdbc.map property overrides other jdbc property"),
  ERROR_0032(
      "0032",
      "Iceberg table does not exist or is in invalid format",
      "Check Snowflake Kafka Connector docs for details"),
  ERROR_0033(
      "0033",
      "Invalid OAuth URL",
      "OAuth URL format: '[http://|https://]<oauth_server>[:<port>][/<path>]'. Protocol defaults"
          + " to 'https://'. Port defaults to 443 for https and 80 for http. Path may contain"
          + " alphanumeric characters, dots, hyphens, and forward slashes (e.g.,"
          + " 'login.example.com/oauth2/v2.0/token')."),
  // Snowflake connection issues 1---
  ERROR_1001(
      "1001",
      "Failed to connect to Snowflake Server",
      "Snowflake connection issue, reported by Snowflake JDBC"),
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

  ERROR_2005("2005", "Failed to close connection", "Failed to close snowflake JDBC connection"),
  ERROR_2006(
      "2006", "Failed to connection status", "Failed to retrieve Snowflake JDBC connection Status"),
  ERROR_2007(
      "2007",
      "Failed to create table",
      "Failed to create table on Snowflake, please check that you have permission to do so."),
  ERROR_2010("2010", "Max retry exceeded", "Api retry exceeded the max retry limit"),
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

  ERROR_5003("5003", "Incompatible table", "Table doesn't have a compatible schema"),
  ERROR_5007(
      "5007",
      "SnowflakeStreamingSinkConnector timeout",
      "SnowflakeStreamingSinkConnector timed out. Tables or stages are not yet "
          + "available for data ingestion to start. If this persists, please "
          + "contact Snowflake support."),
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
      "5015", "Invalid SinkRecord received", "Error parsing SinkRecord value or SinkRecord header"),
  ERROR_5020("5020", "Failed to register MBean in MbeanServer", "Object Name is invalid"),
  ERROR_5022("5022", "Invalid column name", "Failed to find column in the schema"),

  ERROR_5025(
      "5025", "Unexpected data type", "Unexpected data type encountered during schema evolution."),
  ERROR_5026(
      "5026",
      "Invalid SinkRecord received",
      "Cannot infer type from null or empty object/list during schema evolution."),
  ERROR_5027(
      "5027",
      "Data verification failed",
      "Connector couldn't verify that all data was committed to Snowflake. Stopping to avoid data"
          + " loss."),
  ERROR_5028(
      "5028",
      "Failed to open Snowpipe Streaming v2 channel",
      "Failed to open Snowpipe Streaming v2 channel"),
  ERROR_5030(
      "5030",
      "Channel error count threshold exceeded",
      "Channel has reported errors during data ingestion. Check the channel history for details.");

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
