package com.snowflake.kafka.connector.internal.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A fair-scheduling utility that orders items so that unprocessed items are yielded before
 * already-processed ones. Tracks which keys have been processed in the current "epoch." When all
 * items in a working set are already processed, the epoch resets automatically so that no item is
 * permanently starved.
 *
 * <p>Typical usage with a time-budgeted loop:
 *
 * <pre>{@code
 * RoundRobinScheduler<String> scheduler = new RoundRobinScheduler<>();
 *
 * // Each cycle (e.g., preCommit call):
 * scheduler.schedule(partitions, TopicPartition::toString)
 *     .forEach(tp -> {
 *         if (overBudget()) return;
 *         process(tp);
 *         scheduler.markProcessed(tp.toString());
 *     });
 * }</pre>
 *
 * @param <K> the key type used to identify items across calls
 */
public class RoundRobinScheduler<K> {

  private final Set<K> processedInEpoch = new LinkedHashSet<>();

  /**
   * Returns a stream over {@code workingSet} ordered for fair processing: items whose key (as
   * extracted by {@code keyExtractor}) has <em>not</em> been marked processed in the current epoch
   * come first, followed by items that have already been processed.
   *
   * <p>If every item in {@code workingSet} is already marked processed, the epoch is reset
   * automatically before ordering, so all items are treated as unprocessed again.
   *
   * @param <T> the item type
   * @param workingSet the current set of items to schedule
   * @param keyExtractor extracts the scheduling key from an item
   * @return a stream yielding unprocessed items first, then processed items
   */
  public <T> Stream<T> schedule(Collection<T> workingSet, Function<T, K> keyExtractor) {
    if (workingSet.isEmpty()) {
      return Stream.empty();
    }

    boolean allProcessed = true;
    for (T item : workingSet) {
      if (!processedInEpoch.contains(keyExtractor.apply(item))) {
        allProcessed = false;
        break;
      }
    }

    if (allProcessed) {
      processedInEpoch.clear();
      return new ArrayList<>(workingSet).stream();
    }

    List<T> unprocessed = new ArrayList<>();
    List<T> alreadyProcessed = new ArrayList<>();
    for (T item : workingSet) {
      if (processedInEpoch.contains(keyExtractor.apply(item))) {
        alreadyProcessed.add(item);
      } else {
        unprocessed.add(item);
      }
    }

    return Stream.concat(unprocessed.stream(), alreadyProcessed.stream());
  }

  /** Marks a key as processed in the current epoch. */
  public void markProcessed(K key) {
    processedInEpoch.add(key);
  }

  /** Resets the epoch, clearing all processed keys. */
  public void reset() {
    processedInEpoch.clear();
  }
}
