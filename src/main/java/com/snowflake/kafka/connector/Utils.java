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

import net.snowflake.ingest.connection.IngestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Various arbitrary helper functions
 */
public class Utils
{

  //Connector version, change every release
  static final String VERSION = "0.2";

  //Connector parameters
  static final long MAX_RECOVERY_TIME = 10 * 24 * 3600 * 1000; //10 days

  //connector parameter list
  static final String SF_DATABASE = "snowflake.database.name";
  static final String SF_SCHEMA = "snowflake.schema.name";
  static final String SF_USER = "snowflake.user.name";
  static final String SF_PRIVATE_KEY = "snowflake.private.key";
  static final String SF_URL = "snowflake.url.name";
  static final String SF_ROLE = "snowflake.user.role";
  static final String SF_SSL = "sfssl";                 // for test only
  static final String SF_WAREHOUSE = "sfwarehouse";     //for test only

  //constants strings
  static final String KAFKA_OBJECT_PREFIX = "SNOWFLAKE_KAFKA_CONNECTOR";

  static final String DEFAULT_APP_NAME = "DEFAULT_APP_NAME";

  //task id
  static final String TASK_ID = "task_id";

  // applicationName/topicName/partitionNumber
  // /startOffset_endOffset_time_format.json.gz
  private static Pattern FILE_NAME_PATTERN =
    Pattern.compile("^[^/]+/[^/]+/(\\d+)/(\\d+)_(\\d+)_(\\d+)\\.json\\.gz$");

  private static final Logger LOGGER =
    LoggerFactory.getLogger(Utils.class.getName());

  private static String appName = DEFAULT_APP_NAME;

  /**
   * set application name
   *
   * @param name application name
   */
  static void setAppName(String name)
  {
    appName = name;
  }

  /**
   * @return application name
   */
  static String getAppName()
  {
    return appName;
  }

  static String subdirectoryName(String table, int partition)
  {
    return appName + "/" + table + "/" + partition + "/";
  }

  /**
   * @return connector object prefix
   */
  private static String getObjectPrefix()
  {
    return KAFKA_OBJECT_PREFIX + "_" + appName;
  }

  /**
   * count the size of result set
   *
   * @param resultSet
   * @return size
   * @throws SQLException
   */
  static int resultSize(ResultSet resultSet) throws SQLException
  {
    int size = 0;

    while (resultSet.next())
    {
      size++;
    }

    return size;
  }

//  /**
//   * generate table name by given topic
//   * @param topic topic name
//   * @return table name
//   */
//  static String tableName(String topic)
//  {
//    String tableName = getObjectPrefix() + "_TABLE_" + topic;
//
//    LOGGER.debug("generated table name: " + tableName);
//
//    return tableName;
//  }

  /**
   * generate stage name by given table
   *
   * @param table table name
   * @return stage name
   */
  static String stageName(String table)
  {
    String stageName = getObjectPrefix() + "_STAGE_" + table;

    LOGGER.debug("generated stage name: " + stageName);

    return stageName;
  }

  /**
   * generate pipe name by given table and partition
   *
   * @param table     table name
   * @param partition partition name
   * @return pipe name
   */
  static String pipeName(String table, int partition)
  {
    String pipeName = getObjectPrefix() + "_PIPE_" + table + "_" + partition;

    LOGGER.debug("generated pipe name: " + pipeName);

    return pipeName;
  }

  /**
   * generate file name
   * <p>
   * File Name Format:
   * partition/start_end_timeStamp.fileFormat.gz
   * <p>
   * Note: all file names should using the this format
   *
   * @param table     table name
   * @param partition partition number
   * @param start     start offset
   * @param end       end offset
   * @return file name
   */
  static String fileName(String table, int partition, long start, long end)
  {
    long time = System.currentTimeMillis();

    String fileName = appName + "/" + table + "/" + partition + "/" + start +
      "_" + end + "_" + time;

    fileName += ".json.gz";

    LOGGER.debug("generated file name: " + fileName);

    return fileName;
  }

  /**
   * remove .gz from file name.
   * note: for JDBC put use only
   *
   * @param name file name
   * @return file name without .gz
   */
  static String removeGZFromFileName(String name)
  {
    if (name.endsWith(".gz"))
    {
      return name.substring(0, name.length() - 3);
    }

    return name;
  }

  /**
   * convert a timestamp to Date String
   *
   * @param time a long integer representing timestamp
   * @return date string
   */
  static String timestampToDate(long time)
  {
    TimeZone tz = TimeZone.getTimeZone("UTC");

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    df.setTimeZone(tz);

    String date = df.format(new Date(time));

    LOGGER.debug("converted date: " + date);

    return date;
  }

  /**
   * convert ingest status to ingested file status
   *
   * @param status an ingest status
   * @return an ingest file status
   */
  static IngestedFileStatus convertIngestStatus(IngestStatus status)
  {
    switch (status)
    {
      case LOADED:
        return IngestedFileStatus.LOADED;

      case LOAD_IN_PROGRESS:
        return IngestedFileStatus.LOAD_IN_PROGRESS;

      case PARTIALLY_LOADED:
        return IngestedFileStatus.PARTIALLY_LOADED;

      case LOAD_FAILED:

      default:
        return IngestedFileStatus.FAILED;
    }

  }

  /**
   * read start offset from file name
   *
   * @param fileName file name
   * @return start offset
   */
  static long fileNameToStartOffset(String fileName)
  {
    return Long.parseLong(readFromFileName(fileName, 2));
  }

  /**
   * read end offset from file name
   *
   * @param fileName file name
   * @return end offset
   */
  static long fileNameToEndOffset(String fileName)
  {
    return Long.parseLong(readFromFileName(fileName, 3));
  }

  /**
   * read ingested time from file name
   *
   * @param fileName file name
   * @return ingested time
   */
  static long fileNameToTimeIngested(String fileName)
  {
    return Long.parseLong(readFromFileName(fileName, 4));
  }

  /**
   * read partition index from file name
   *
   * @param fileName file name
   * @return partition index
   */
  static int fileNameToPartition(String fileName)
  {
    return Integer.parseInt(readFromFileName(fileName, 1));
  }

  /**
   * @return current UTC time stamp
   */
  static long currentTime()
  {
    return System.currentTimeMillis();
  }

  /**
   * check whether the given file is expired
   *
   * @param fileName file name
   * @return true if expired, otherwise false
   */
  static boolean isFileExpired(String fileName)
  {
    return currentTime() - fileNameToTimeIngested(fileName)
      > Utils.MAX_RECOVERY_TIME;
  }

  /**
   * @return query mark of sink connector
   */
  static String getConnectorQueryMark()
  {
    return " /* kafka, " + appName + ", connector */";
  }

  /**
   * @return query mark of sink task
   */
  static String getTaskQueryMark(String id)
  {
    return " /* kafka, " + appName + ", task " + id + " */";
  }

  /**
   * @return query mark of IT test
   */
  static String getTestQueryMark()
  {
    return " /* kafka, test */";
  }

  /**
   * read a value from file name
   *
   * @param fileName file name
   * @param index    value index
   * @return string value
   */
  private static String readFromFileName(String fileName, int index)
  {
    Matcher matcher = FILE_NAME_PATTERN.matcher(fileName);

    if (!matcher.find())
    {
      LOGGER.error("invalid file name {}", fileName);
      throw new IllegalArgumentException("invalid file name " + fileName);
    }

    return matcher.group(index);
  }

  /**
   * verify file name
   *
   * @param fileName file name
   * @return true if file name format is correct, false otherwise
   */
  static boolean verifyFileName(String fileName)
  {
    return FILE_NAME_PATTERN.matcher(fileName).find();
  }

  /**
   * Ensure a name wrapped with double quotes, majorly used in table name
   *
   * @param name name string with or without double quotes
   * @return name with double quotes
   */
  static String ensureQuoted(String name)
  {
    //todo: need trim or not ?
    if (name.startsWith("\"") && name.endsWith("\""))
    {
      return name;
    }
    else
    {
      return "\"" + name + "\"";
    }
  }

  /**
   * ingested file status
   * some status are grouped as 'finalized' status (LOADED, PARTIALLY_LOADED,
   * FAILED) -- we can purge these files
   * others are grouped as 'not_finalized'
   */
  public enum IngestedFileStatus    // for ingest sdk
  {
    LOADED,
    PARTIALLY_LOADED,
    FAILED,
    LATEST_FILE_STATUS_FINALIZED,         // finalized ==> one of loaded,
    // partially_loaded, or failed
    LATEST_FILE_STATUS_NOT_FINALIZED,     // not_finalized ==> !finalized
    LOAD_IN_PROGRESS,
    EXPIRED,
    NOT_FOUND,
  }


}
