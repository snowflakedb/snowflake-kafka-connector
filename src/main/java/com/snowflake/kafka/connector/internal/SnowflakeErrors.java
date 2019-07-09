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

public enum SnowflakeErrors
{

  // connector configuration issues 0---
  ERROR_0001(
      "0001",
      "Invalid input connector configuration",
      "input kafka connector configuration is null, missing required values, " +
          "or wrong input value"
  ),
  ERROR_0002(
      "0002",
      "Invalid private key",
      "private key should be a valid PEM RSA private key"
  ),
  ERROR_0003(
      "0003",
      "Missing required parameter",
      "one or multiple required parameters haven't be provided"
  ),
  ERROR_0004(
      "0004",
      "Empty Stage name",
      "Input Stage name is empty string or null"
  ),
  ERROR_0005(
      "0005",
      "Empty Table name",
      "Input Table name is empty string or null"
  ),
  ERROR_0006(
      "0006",
      "Empty Pipe name",
      "Input Pipe name is empty String or null"
  ),
  ERROR_0007(
      "0007",
      "Invalid Snowflake URL",
      "Snowflake URL format: 'https://<account_name>.<region_name>" +
          ".snowflakecomputing.com:443', 'https://' and ':443' are optional."
  ),
  ERROR_0008(
      "0008",
      "Invalid staging file name",
      "File name format: <app_name>/<table_name>/<partition_number" +
          ">/<start_offset>_<end_offset>_<timestamp>.json.gz"
  ),
  ERROR_0009(
      "0009",
      "Invalid value converter",
      "Only support Snowflake Converters (e.g. SnowflakeJsonConverter, " +
          "SnowflakeAvroConverter)"
  ),
  ERROR_0010(
      "0010",
      "Invalid input record",
      "Input record value can't be parsed"
  ),
  ERROR_0011(
      "0011",
      "Failed to load schema from Schema Registry",
      "Schema ID doesn't exist"
  ),
  ERROR_0012(
      "0012",
      "Failed to connect schema registry service",
      "Schema registry service is not available"
  ),
  // Snowflake connection issues 1---
  ERROR_1001(
      "1001",
      "Failed to connect to Snowflake Server",
      "Snowflake connection issue, reported by Snowflake JDBC"
  ),
  ERROR_1002(
      "1002",
      "Failed to connect to Snowflake Ingestion Service Endpoint",
      "Ingestion Service connection issue, reported by Ingest SDK"
  ),
  ERROR_1003(
      "1003",
      "Snowflake connection is closed",
      "Either the current connection is closed or hasn't connect to snowflake" +
          " server"
  ),
  // SQL issues 2---
  ERROR_2001(
      "2001",
      "Failed to prepare SQL statement",
      "SQL Exception, reported by Snowflake JDBC"
  ),

  ERROR_2002(
      "2002",
      "Failed to download file",
      "Failed to download file from Snowflake Stage though JDBC"
  ),

  ERROR_2003(
      "2003",
      "Failed to upload file",
      "Failed to upload file to Snowflake Stage though JDBC"
  ),
  ERROR_2004(
      "2004",
      "Failed to retrieve list of files",
      "Failed to retrieve list of files on Stage though JDBC"
  ),

  // Snowpipe related issues 3---
  ERROR_3001(
      "3001",
      "Failed to ingest file",
      "Exception reported by Ingest SDK"
  ),

  ERROR_3002(
      "3002",
      "Failed to read load history",
      "Exception reported by Ingest SDK"
  ),

  ERROR_3003(
      "3003",
      "Timeout when checking ingest report",
      "Exceed respond time limitation"
  ),

  ERROR_3004(
      "3004",
      "Snowpipe is not found",
      "Snowpipe object should be created in the connector and registered " +
          "to pipe list"
  ),
  ERROR_3005(
      "3005",
      "Failed to create pipe",
      "User may have insufficient privileges. If this persists, please " +
          "contact Snowflake support. "
  ),

  // Wrong result issues 4---
  ERROR_4001(
      "4001",
      "Unexpected Result",
      "Get wrong results from Snowflake service"
  ),
  //Connector internal errors 5---
  ERROR_5001(
      "5001",
      "Failed to create json encoder",
      "Failed to create json encoder in Snowflake Avro Converter"
  ),
  ERROR_5002(
      "5002",
      "Not support data source connector",
      "Snowflake converter doesn't support data source connector yet"
  ),
  ERROR_5003(
      "5003",
      "Incompatible table",
      "Table doesn't have a compatible schema"
  ),
  ERROR_5004(
      "5004",
      "Incompatible stage",
      "Stage contains files that were not created by the Snowflake Kafka " +
          "Connector. The stage needs a careful cleanup."
  ),
  ERROR_5005(
      "5005",
      "Incompatible pipe",
      "Pipe exists, but is incompatable with Snowflake Kafka Connector and " +
          "must be dropped before restarting the connector."
  ),
  ERROR_5006(
      "5006",
      "Connector stopped due to invalid configuration",
      "Exception while creating or validating table, stage, or pipe."
  ),
  ERROR_5007(
      "5007",
      "SnowflakeSinkConnector timeout",
      "SnowflakeSinkConnector timed out. Tables or stages are not yet " +
          "available for data ingestion to start. If this persists, please " +
          "contact Snowflake support."
  ),
  ERROR_5008(
      "5008",
      "SnowflakeSinkTask timeout",
      "SnowflakeSinkConnector timed out. Tables or stages are not yet " +
          "available for data ingestion to start. If this persists, please " +
          "contact Snowflake support."
  ),
  ERROR_5009(
      "5009",
      "Partition is not found in task buffer",
      "This should not happen, please contact Snowflake support."
  );


  //properties

  private final String name;
  private final String detail;
  private final String code;

  SnowflakeErrors(String code, String name, String detial)
  {
    this.code = code;
    this.name = name;
    this.detail = detial;
  }

  public SnowflakeKafkaConnectorException getException()
  {
    return new SnowflakeKafkaConnectorException(toString());
  }

  public SnowflakeKafkaConnectorException getException(Object msg)
  {
    if (msg instanceof Exception)
    {
      Exception e = (Exception) msg;
      String str = e.getMessage();

      for (StackTraceElement element : e.getStackTrace())
      {
        str += "\n" + element.toString();

      }

      return new SnowflakeKafkaConnectorException(
          Logging.logMessage("Exception: {}\nError Code: {}\nDetail: " +
              "{}\nError " +
              "Message: " + str, name, code, detail));
    }

    return new SnowflakeKafkaConnectorException(
        Logging.logMessage("Exception: {}\nError Code: {}\nDetail: {}\nError " +
            "Message: {}", name, code, detail, msg.toString())
    );
  }

  public String getDetail()
  {
    return this.detail;
  }

  @Override
  public String toString()
  {
    return Logging.logMessage("Exception: {}\nError Code: {}\nDetail: {}",
        name, code, detail);
  }
}

