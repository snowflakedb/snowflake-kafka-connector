package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ValidationResultMapperTest {

  @Test
  void mapWithExtraColumnsAndBothNotNullViolations() {
    Set<String> extraCols = new HashSet<>(Arrays.asList("NEW_COL1", "NEW_COL2"));
    Set<String> missingNotNull = new HashSet<>(Arrays.asList("REQUIRED_COL"));
    Set<String> nullNotNull = new HashSet<>(Arrays.asList("NULLABLE_COL"));

    ValidationResult result =
        ValidationResult.structuralError(extraCols, missingNotNull, nullNotNull);

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "MY_TABLE");

    assertEquals("MY_TABLE", items.getTableName());
    assertThat(items.getColumnsToAdd()).containsExactlyInAnyOrder("\"NEW_COL1\"", "\"NEW_COL2\"");
    assertThat(items.getColumnsToDropNonNullability())
        .containsExactlyInAnyOrder("\"REQUIRED_COL\"", "\"NULLABLE_COL\"");
    assertTrue(items.hasDataForSchemaEvolution());
  }

  @Test
  void mapWithEmptyResult() {
    ValidationResult result =
        ValidationResult.structuralError(
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "MY_TABLE");

    assertFalse(items.hasDataForSchemaEvolution());
    assertThat(items.getColumnsToAdd()).isEmpty();
    assertThat(items.getColumnsToDropNonNullability()).isEmpty();
  }

  @Test
  void mapWithOnlyExtraColumns() {
    ValidationResult result =
        ValidationResult.structuralError(
            new HashSet<>(Arrays.asList("COL1")), Collections.emptySet(), Collections.emptySet());

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "T");

    assertThat(items.getColumnsToAdd()).containsExactly("\"COL1\"");
    assertThat(items.getColumnsToDropNonNullability()).isEmpty();
  }

  @Test
  void mapWithOnlyMissingNotNull() {
    ValidationResult result =
        ValidationResult.structuralError(
            Collections.emptySet(), new HashSet<>(Arrays.asList("COL1")), Collections.emptySet());

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "T");

    assertThat(items.getColumnsToAdd()).isEmpty();
    assertThat(items.getColumnsToDropNonNullability()).containsExactly("\"COL1\"");
  }

  @Test
  void mapWithOnlyNullValueForNotNull() {
    ValidationResult result =
        ValidationResult.structuralError(
            Collections.emptySet(), Collections.emptySet(), new HashSet<>(Arrays.asList("COL1")));

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "T");

    assertThat(items.getColumnsToAdd()).isEmpty();
    assertThat(items.getColumnsToDropNonNullability()).containsExactly("\"COL1\"");
  }

  @Test
  void mapCombinesBothNotNullViolationTypes() {
    ValidationResult result =
        ValidationResult.structuralError(
            Collections.emptySet(),
            new HashSet<>(Arrays.asList("MISSING1")),
            new HashSet<>(Arrays.asList("NULL1")));

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "T");

    assertThat(items.getColumnsToDropNonNullability())
        .containsExactlyInAnyOrder("\"MISSING1\"", "\"NULL1\"");
  }
}
