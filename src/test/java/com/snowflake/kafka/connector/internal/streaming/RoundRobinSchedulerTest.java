package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RoundRobinSchedulerTest {

  private RoundRobinScheduler<String> scheduler;

  @BeforeEach
  void setUp() {
    scheduler = new RoundRobinScheduler<>();
  }

  @Test
  void allUnprocessed_returnsOriginalOrder() {
    List<String> items = Arrays.asList("A", "B", "C", "D");

    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Arrays.asList("A", "B", "C", "D"), result);
  }

  @Test
  void someProcessed_unprocessedComeFirst() {
    scheduler.markProcessed("A");
    scheduler.markProcessed("B");

    List<String> items = Arrays.asList("A", "B", "C", "D");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Arrays.asList("C", "D", "A", "B"), result);
  }

  @Test
  void allProcessed_epochResetsAndReturnsAll() {
    scheduler.markProcessed("A");
    scheduler.markProcessed("B");
    scheduler.markProcessed("C");
    scheduler.markProcessed("D");

    List<String> items = Arrays.asList("A", "B", "C", "D");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Arrays.asList("A", "B", "C", "D"), result);
  }

  @Test
  void emptyWorkingSet_returnsEmptyStream() {
    List<String> result =
        scheduler
            .schedule(Collections.emptyList(), Function.identity())
            .collect(Collectors.toList());

    assertTrue(result.isEmpty());
  }

  @Test
  void processedKeysNotInWorkingSet_noEffectOnOrdering() {
    scheduler.markProcessed("X");
    scheduler.markProcessed("Y");

    List<String> items = Arrays.asList("A", "B", "C");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Arrays.asList("A", "B", "C"), result);
  }

  @Test
  void workingSetChanges_newItemsGetPriority() {
    scheduler.markProcessed("A");
    scheduler.markProcessed("B");

    List<String> items = Arrays.asList("A", "B", "C", "D", "E");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Arrays.asList("C", "D", "E", "A", "B"), result);
  }

  @Test
  void workingSetShrinks_removedItemsIgnored() {
    scheduler.markProcessed("A");
    scheduler.markProcessed("B");
    scheduler.markProcessed("C");

    List<String> items = Arrays.asList("B", "D");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Arrays.asList("D", "B"), result);
  }

  @Test
  void reset_clearsProcessedState() {
    scheduler.markProcessed("A");
    scheduler.markProcessed("B");
    scheduler.reset();

    List<String> items = Arrays.asList("A", "B", "C");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Arrays.asList("A", "B", "C"), result);
  }

  @Test
  void multipleEpochs_fairnessAcrossCycles() {
    List<String> items = Arrays.asList("A", "B", "C", "D");

    // Epoch 1, call 1: process A, B only (simulating time budget cutoff)
    List<String> call1 =
        scheduler.schedule(items, Function.identity()).collect(Collectors.toList());
    assertEquals(Arrays.asList("A", "B", "C", "D"), call1);
    scheduler.markProcessed("A");
    scheduler.markProcessed("B");

    // Epoch 1, call 2: C, D should come first since they weren't processed
    List<String> call2 =
        scheduler.schedule(items, Function.identity()).collect(Collectors.toList());
    assertEquals(Arrays.asList("C", "D", "A", "B"), call2);
    scheduler.markProcessed("C");
    scheduler.markProcessed("D");

    // Epoch 2 (auto-reset): all processed, epoch resets
    List<String> call3 =
        scheduler.schedule(items, Function.identity()).collect(Collectors.toList());
    assertEquals(Arrays.asList("A", "B", "C", "D"), call3);
  }

  @Test
  void keyExtractor_worksWithComplexObjects() {
    List<int[]> items =
        Arrays.asList(new int[] {1, 10}, new int[] {2, 20}, new int[] {3, 30}, new int[] {4, 40});
    Function<int[], Integer> keyExtractor = arr -> arr[0];

    scheduler = null; // can't reuse String scheduler
    RoundRobinScheduler<Integer> intScheduler = new RoundRobinScheduler<>();

    intScheduler.markProcessed(1);
    intScheduler.markProcessed(3);

    List<int[]> result = intScheduler.schedule(items, keyExtractor).collect(Collectors.toList());

    assertEquals(2, result.get(0)[0]);
    assertEquals(4, result.get(1)[0]);
    assertEquals(1, result.get(2)[0]);
    assertEquals(3, result.get(3)[0]);
  }

  @Test
  void singleItem_processedResetsEpoch() {
    List<String> items = Collections.singletonList("A");

    scheduler.markProcessed("A");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    assertEquals(Collections.singletonList("A"), result);
  }

  @Test
  void allItemsInWorkingSetContain_processedFromSuperset() {
    // Processed set is a superset of working set
    scheduler.markProcessed("A");
    scheduler.markProcessed("B");
    scheduler.markProcessed("C");
    scheduler.markProcessed("D");
    scheduler.markProcessed("E");

    List<String> items = Arrays.asList("B", "D");
    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    // All items in working set are processed -> epoch resets
    assertEquals(Arrays.asList("B", "D"), result);
  }

  @Test
  void preservesRelativeOrder_withinGroups() {
    // Use a LinkedHashSet-backed list to ensure deterministic input order
    List<String> items = Arrays.asList("D", "C", "B", "A");

    scheduler.markProcessed("C");
    scheduler.markProcessed("A");

    List<String> result = scheduler.schedule(items, Function.identity()).collect(Collectors.toList());

    // Unprocessed: D, B (in input order). Processed: C, A (in input order).
    assertEquals(Arrays.asList("D", "B", "C", "A"), result);
  }

  @Test
  void streamIsNotConsumedEagerly() {
    List<String> items = Arrays.asList("A", "B", "C", "D");
    scheduler.markProcessed("A");

    Set<String> visited = new LinkedHashSet<>();
    scheduler
        .schedule(items, Function.identity())
        .peek(visited::add)
        .limit(2)
        .collect(Collectors.toList());

    assertEquals(2, visited.size());
    assertTrue(visited.contains("B"));
    assertTrue(visited.contains("C"));
  }
}
