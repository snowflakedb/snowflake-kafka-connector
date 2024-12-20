package com.snowflake.kafka.connector.internal;

import com.google.common.base.Strings;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.utils.Crc32C;

public class FileNameUtils {
  private static final KCLogger LOGGER = new KCLogger(FileNameUtils.class.getName());

  /**
   * generate file name
   *
   * @param prefix prefix
   * @param start start offset
   * @param end end offset
   * @return file name
   */
  static String fileName(String prefix, long start, long end) {
    long time = System.currentTimeMillis();
    String fileName = prefix + start + "_" + end + "_" + time + ".json.gz";
    LOGGER.debug("generated file name: {}", fileName);
    return fileName;
  }

  /**
   * generate file name for broken data
   *
   * @param prefix prefix
   * @param offset record offset
   * @param isKey is the broken record a key or a value
   * @return file name
   */
  static String brokenRecordFileName(String prefix, long offset, boolean isKey) {
    long time = System.currentTimeMillis();
    String isKeyString = isKey ? "key" : "value";
    String fileName = prefix + offset + "_" + isKeyString + "_" + time + ".gz";
    LOGGER.debug("generated broken data file name: {}", fileName);
    return fileName;
  }

  /**
   * generate file prefix
   *
   * @param appName connector name
   * @param table table name
   * @param topic topic name
   * @param partition partition index
   * @return file prefix
   */
  static String filePrefix(String appName, String table, String topic, int partition) {
    if (partition >= 0x8000) {
      throw new IllegalArgumentException(
          String.format("partition id=%d is too large (max=%d)", partition, 0x8000));
    }
    return appName + "/" + table + "/" + calculatePartitionPart(topic, partition) + "/";
  }

  private static BigInteger calculatePartitionPart(String topic, int partition) {
    BigInteger partitionPart = BigInteger.valueOf(partition);
    if (!Strings.isNullOrEmpty(topic)) {
      // if topic is provided as part of the file prefix,
      // 1. let's calculate stable hash code out of it,
      // 2. bit shift it by 16 bits left,
      // 3. add 0x8000 (light up 15th bit as a marker)
      // 4. add partition id (which should in production use cases never reach a value above 5.000
      // partitions per topic).
      // In theory - we would support 32767 partitions, which is more than any reasonable value for
      // a single topic
      byte[] bytes = topic.toUpperCase().getBytes(StandardCharsets.UTF_8);
      BigInteger hash = BigInteger.valueOf(Crc32C.compute(bytes, 0, bytes.length));
      partitionPart =
          hash.abs()
              .multiply(BigInteger.valueOf(0x10000))
              .add(BigInteger.valueOf(0x8000))
              .add(partitionPart);
    }
    return partitionPart;
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
  static boolean verifyFileName(String fileName) {
    return FILE_NAME_PATTERN.matcher(fileName).find();
  }

  /**
   * read start offset from file name
   *
   * @param fileName file name
   * @return start offset
   */
  static long fileNameToStartOffset(String fileName) {
    return Long.parseLong(readFromFileName(fileName, 2));
  }

  /**
   * read end offset from file name
   *
   * @param fileName file name
   * @return end offset
   */
  static long fileNameToEndOffset(String fileName) {
    return Long.parseLong(readFromFileName(fileName, 3));
  }

  /**
   * read ingested time from file name
   *
   * @param fileName file name
   * @return ingested time
   */
  static long fileNameToTimeIngested(String fileName) {
    return Long.parseLong(readFromFileName(fileName, 4));
  }

  /**
   * read partition index from file name
   *
   * @param fileName file name
   * @return partition index
   */
  static int fileNameToPartition(String fileName) {
    BigInteger value = new BigInteger(readFromFileName(fileName, 1));
    return value.and(BigInteger.valueOf(0x7FFF)).intValue();
  }

  /**
   * remove prefix and .gz from file name. note: for JDBC put use only
   *
   * @param name file name
   * @return file name without .gz
   */
  static String removePrefixAndGZFromFileName(String name) {
    if (name == null || name.isEmpty() || name.endsWith("/")) {
      throw SnowflakeErrors.ERROR_0008.getException("input file name: " + name);
    }

    if (name.endsWith(".gz")) {
      name = name.substring(0, name.length() - 3);
    }

    int prefixEndIndex = name.lastIndexOf('/');
    if (prefixEndIndex > -1) {
      return name.substring(prefixEndIndex + 1, name.length());
    }

    return name;
  }

  /**
   * Get the prefix from the file name note: for JDBC put use only
   *
   * @param name file name
   * @return prefix from the
   */
  static String getPrefixFromFileName(String name) {
    if (name == null || name.isEmpty() || name.endsWith("/")) {
      throw SnowflakeErrors.ERROR_0008.getException("input file name: " + name);
    }

    int prefixEndIndex = name.lastIndexOf('/');
    if (prefixEndIndex > -1) {
      return name.substring(0, prefixEndIndex);
    }
    return null;
  }

  /**
   * read a value from file name
   *
   * @param fileName file name
   * @param index value index
   * @return string value
   */
  private static String readFromFileName(String fileName, int index) {
    Matcher matcher = FILE_NAME_PATTERN.matcher(fileName);

    if (!matcher.find()) {
      throw SnowflakeErrors.ERROR_0008.getException("input file name: " + fileName);
    }

    return matcher.group(index);
  }

  /**
   * Find gaps in offset ranges.
   *
   * @param filenames list of files
   * @return continuous and missing offsets for given filenames
   */
  static OffsetContinuityRanges searchForMissingOffsets(List<String> filenames) {
    List<Pair<Long, Long>> missingOffsets = new ArrayList<>();

    List<Pair<Long, Long>> continuousOffsets =
        filenames.stream()
            .map(
                file ->
                    Pair.of(
                        FileNameUtils.fileNameToStartOffset(file),
                        FileNameUtils.fileNameToEndOffset(file)))
            .sorted()
            .collect(Collectors.toList());

    for (int i = 1; i < continuousOffsets.size(); i++) {
      Pair<Long, Long> current = continuousOffsets.get(i);
      Pair<Long, Long> previous = continuousOffsets.get(i - 1);

      if (previous.getRight() + 1 != current.getLeft()) {
        missingOffsets.add(Pair.of(previous.getRight() + 1, current.getLeft() - 1));
      }
    }
    return new OffsetContinuityRanges(continuousOffsets, missingOffsets);
  }
}
