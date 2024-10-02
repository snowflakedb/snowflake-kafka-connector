package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.FileNameUtils.filePrefix;

public class FileNameTestUtils {
  private FileNameTestUtils() {}

  // Used for testing only
  static String fileName(
      String appName, String table, String topic, int partition, long start, long end, long time) {
    return filePrefix(appName, table, topic, partition)
        + start
        + "_"
        + end
        + "_"
        + time
        + ".json.gz";
  }

  /**
   * generate file name File Name Format: app/table/partition/start_end_timeStamp.fileFormat.gz
   * Note: all file names should using the this format
   *
   * @param appName connector name
   * @param table table name
   * @param topic name
   * @param partition partition number
   * @param start start offset
   * @param end end offset
   * @return file name
   */
  public static String fileName(
      String appName, String table, String topic, int partition, long start, long end) {
    String prefix = filePrefix(appName, table, topic, partition);
    return FileNameUtils.fileName(prefix, start, end);
  }

  /**
   * generate file name for broken data
   *
   * @param appName app name
   * @param table table name
   * @param partition partition id
   * @param offset record offset
   * @param isKey is the broken record a key or a value
   * @return file name
   */
  static String brokenRecordFileName(
      String appName, String table, String topic, int partition, long offset, boolean isKey) {
    return FileNameUtils.brokenRecordFileName(
        filePrefix(appName, table, topic, partition), offset, isKey);
  }

  /**
   * check whether the given file is expired
   *
   * @param fileName file name
   * @return true if expired, otherwise false
   */
  static boolean isFileExpired(String fileName) {
    return System.currentTimeMillis() - FileNameUtils.fileNameToTimeIngested(fileName)
        > InternalUtils.MAX_RECOVERY_TIME;
  }
}
