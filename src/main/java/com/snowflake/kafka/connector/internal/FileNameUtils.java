package com.snowflake.kafka.connector.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class FileNameUtils
{
  private static final Logger LOGGER =
    LoggerFactory.getLogger(FileNameUtils.class.getName());

  /**
   * generate file name
   * File Name Format:
   * app/table/partition/start_end_timeStamp.fileFormat.gz
   * Note: all file names should using the this format
   *
   * @param appName   connector name
   * @param table     table name
   * @param partition partition number
   * @param start     start offset
   * @param end       end offset
   * @return file name
   */
  static String fileName(String appName, String table, int partition,
                                long start, long end)
  {
    return fileName(filePrefix(appName, table, partition), start, end);
  }


  /**
   * generate file name
   * @param prefix prefix
   * @param start start offset
   * @param end end offset
   * @return file name
   */
  static String fileName(String prefix, long start, long end)
  {
    long time = System.currentTimeMillis();
    String fileName = prefix + start + "_" + end + "_" + time + ".json.gz";
    LOGGER.debug(Logging.logMessage("generated file name: {}", fileName));
    return fileName;
  }

  /**
   * generate file name for broken data
   * @param appName app name
   * @param table table name
   * @param partition partition id
   * @param offset record offset
   * @return file name
   */
  static String brokenRecordFileName(String appName, String table, int partition, long offset)
  {
    return brokenRecordFileName(filePrefix(appName, table, partition), offset);
  }

  /**
   * generate file name for broken data
   * @param prefix prefix
   * @param offset record offset
   * @return file name
   */
  static String brokenRecordFileName(String prefix, long offset)
  {
    long time = System.currentTimeMillis();
    String fileName = prefix + offset + "_" + time + ".gz";
    LOGGER.debug(Logging.logMessage("generated broken data file name: {}", fileName));
    return fileName;
  }

  /**
   * generate file prefix
   * @param appName connector name
   * @param table table name
   * @param partition partition index
   * @return file prefix
   */
  static String filePrefix(String appName, String table, int partition)
  {
    return appName + "/" + table + "/" + partition + "/";
  }

  // applicationName/tableName/partitionNumber
  // /startOffset_endOffset_time_format.json.gz
  private static Pattern FILE_NAME_PATTERN =
    Pattern.compile("^[^/]+/[^/]+/(\\d+)/(\\d+)_(\\d+)_(\\d+)\\.json\\.gz$");
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
   * check whether the given file is expired
   *
   * @param fileName file name
   * @return true if expired, otherwise false
   */
  static boolean isFileExpired(String fileName)
  {
    return System.currentTimeMillis() - fileNameToTimeIngested(fileName)
      > InternalUtils.MAX_RECOVERY_TIME;
  }

  /**
   * remove prefix and .gz from file name.
   * note: for JDBC put use only
   *
   * @param name file name
   * @return file name without .gz
   */
  static String removePrefixAndGZFromFileName(String name)
  {
    if (name == null || name.isEmpty() || name.endsWith("/"))
    {
      throw SnowflakeErrors.ERROR_0008.getException("input file name: " +
              name);
    }

    if (name.endsWith(".gz"))
    {
      name = name.substring(0, name.length() - 3);
    }

    int prefixEndIndex = name.lastIndexOf('/');
    if (prefixEndIndex > -1)
    {
      return name.substring(prefixEndIndex + 1, name.length());
    }

    return name;
  }

  /**
   * Get the prefix from the file name
   * note: for JDBC put use only
   *
   * @param name file name
   * @return prefix from the
   */
  static String getPrefixFromFileName(String name)
  {
    if (name == null || name.isEmpty() || name.endsWith("/"))
    {
      throw SnowflakeErrors.ERROR_0008.getException("input file name: " +
              name);
    }

    int prefixEndIndex = name.lastIndexOf('/');
    if (prefixEndIndex > -1)
    {
      return name.substring(0, prefixEndIndex);
    }
    return null;
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
      throw SnowflakeErrors.ERROR_0008.getException("input file name: " +
        fileName);
    }

    return matcher.group(index);
  }

}
