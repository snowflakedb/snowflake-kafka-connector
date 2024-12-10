package com.snowflake.kafka.connector.internal;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class OffsetContinuityRanges {
  private final List<Pair<Long, Long>> continuousOffsets;
  private final List<Pair<Long, Long>> missingOffsets;

  public OffsetContinuityRanges(
      List<Pair<Long, Long>> continuousOffsets, List<Pair<Long, Long>> missingOffsets) {
    this.continuousOffsets = continuousOffsets;
    this.missingOffsets = missingOffsets;
  }

  public String getContinuousOffsets() {
    return parseList(continuousOffsets);
  }

  public String getMissingOffsets() {
    return parseList(missingOffsets);
  }

  private static String parseList(List<Pair<Long, Long>> list) {
    return list.stream()
        .map(range -> "[" + range.getLeft() + "," + range.getRight() + "]")
        .collect(Collectors.joining("", "[", "]"));
  }
}
