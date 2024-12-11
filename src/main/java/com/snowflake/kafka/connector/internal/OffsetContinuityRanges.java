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
    return serializeList(continuousOffsets);
  }

  public String getMissingOffsets() {
    return serializeList(missingOffsets);
  }

  private static String serializeList(List<Pair<Long, Long>> list) {
    return list.stream()
        .map(range -> "[" + range.getLeft() + "," + range.getRight() + "]")
        .collect(Collectors.joining("", "[", "]"));
  }
}
