package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SchemaEvolutionTargetItemsTest {

  @Test
  void hasDataForSchemaEvolution_withColumnsToAdd() {
    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "table", Collections.emptySet(), new HashSet<>(Arrays.asList("COL1")));
    assertTrue(items.hasDataForSchemaEvolution());
  }

  @Test
  void hasDataForSchemaEvolution_withColumnsToDropNonNull() {
    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "table", new HashSet<>(Arrays.asList("COL1")), Collections.emptySet());
    assertTrue(items.hasDataForSchemaEvolution());
  }

  @Test
  void hasDataForSchemaEvolution_empty() {
    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems("table", Collections.emptySet(), Collections.emptySet());
    assertFalse(items.hasDataForSchemaEvolution());
  }

  @Test
  void constructorHandlesNullSets() {
    SchemaEvolutionTargetItems items = new SchemaEvolutionTargetItems("table", null, null);
    assertThat(items.getColumnsToAdd()).isEmpty();
    assertThat(items.getColumnsToDropNonNullability()).isEmpty();
    assertFalse(items.hasDataForSchemaEvolution());
  }

  @Test
  void twoArgConstructorSetsColumnsToAdd() {
    Set<String> cols = new HashSet<>(Arrays.asList("A", "B"));
    SchemaEvolutionTargetItems items = new SchemaEvolutionTargetItems("table", cols);
    assertThat(items.getColumnsToAdd()).containsExactlyInAnyOrder("A", "B");
    assertThat(items.getColumnsToDropNonNullability()).isEmpty();
  }

  @Test
  void gettersReturnUnmodifiableSets() {
    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "table", new HashSet<>(Arrays.asList("DROP1")), new HashSet<>(Arrays.asList("ADD1")));
    assertThrows(UnsupportedOperationException.class, () -> items.getColumnsToAdd().add("X"));
    assertThrows(
        UnsupportedOperationException.class, () -> items.getColumnsToDropNonNullability().add("X"));
  }

  @Test
  void defensiveCopyPreventsExternalMutation() {
    Set<String> original = new HashSet<>(Arrays.asList("COL1"));
    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems("table", original, Collections.emptySet());
    original.add("COL2");
    assertThat(items.getColumnsToDropNonNullability()).containsExactly("COL1");
  }

  @Test
  void equalityAndHashCode() {
    SchemaEvolutionTargetItems a =
        new SchemaEvolutionTargetItems(
            "t", new HashSet<>(Arrays.asList("C1")), new HashSet<>(Arrays.asList("C2")));
    SchemaEvolutionTargetItems b =
        new SchemaEvolutionTargetItems(
            "t", new HashSet<>(Arrays.asList("C1")), new HashSet<>(Arrays.asList("C2")));
    SchemaEvolutionTargetItems c =
        new SchemaEvolutionTargetItems("t", Collections.emptySet(), Collections.emptySet());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }
}
