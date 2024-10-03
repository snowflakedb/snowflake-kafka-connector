package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class InsertErrorMapperTest {

  private final InsertErrorMapper mapper = new InsertErrorMapper();

  @Test
  void shouldMapErrorToProperItems() {
    InsertValidationResponse.InsertError insertError =
        createInsertError(
            Collections.singletonList("extraCol"),
            Arrays.asList("missingNotNullCol", "missingNotNullCol2"),
            Arrays.asList("nullValueForNotNull", "nullValueForNotNull2"));
    SchemaEvolutionTargetItems items = mapper.mapToSchemaEvolutionItems(insertError, "tableName");

    assertThat(items.getTableName()).isEqualTo("tableName");
    assertThat(items.getColumnsToDropNonNullability())
        .containsExactlyInAnyOrder(
            "missingNotNullCol",
            "missingNotNullCol2",
            "nullValueForNotNull",
            "nullValueForNotNull2");
    assertThat(items.getColumnsToAdd()).containsExactly("extraCol");
    assertThat(items.hasDataForSchemaEvolution()).isTrue();
  }

  @Test
  void shouldReturnNoDataForEmptyLists() {
    InsertValidationResponse.InsertError insertError =
        createInsertError(
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    SchemaEvolutionTargetItems items = mapper.mapToSchemaEvolutionItems(insertError, "tableName");

    assertThat(items.getTableName()).isEqualTo("tableName");
    assertThat(items.getColumnsToDropNonNullability()).isEmpty();
    assertThat(items.getColumnsToAdd()).isEmpty();
    assertThat(items.hasDataForSchemaEvolution()).isFalse();
  }

  @Test
  void shouldReturnNoDataForNullLists() {
    InsertValidationResponse.InsertError insertError = createInsertError(null, null, null);
    SchemaEvolutionTargetItems items = mapper.mapToSchemaEvolutionItems(insertError, "tableName");

    assertThat(items.getTableName()).isEqualTo("tableName");
    assertThat(items.getColumnsToDropNonNullability()).isEmpty();
    assertThat(items.getColumnsToAdd()).isEmpty();
    assertThat(items.hasDataForSchemaEvolution()).isFalse();
  }

  @ParameterizedTest(name = "should return true when one list has data: {0}")
  @MethodSource("provideTestCases")
  void shouldReturnTrueWhenOneListHasData(
      InsertValidationResponse.InsertError insertError,
      String[] expectedColumnsToDropNonNullability,
      String[] expectedColumnsToAdd) {
    SchemaEvolutionTargetItems items = mapper.mapToSchemaEvolutionItems(insertError, "tableName");

    assertThat(items.getTableName()).isEqualTo("tableName");
    assertThat(items.getColumnsToDropNonNullability())
        .containsExactlyInAnyOrder(expectedColumnsToDropNonNullability);
    assertThat(items.getColumnsToAdd()).containsExactlyInAnyOrder(expectedColumnsToAdd);
    assertThat(items.hasDataForSchemaEvolution()).isTrue();
  }

  private static Stream<Arguments> provideTestCases() {
    return Stream.of(
        Arguments.of(
            createInsertError(Collections.singletonList("extraCol"), null, null),
            new String[] {},
            new String[] {"extraCol"}),
        Arguments.of(
            createInsertError(null, Collections.singletonList("missingNotNull"), null),
            new String[] {"missingNotNull"},
            new String[] {}),
        Arguments.of(
            createInsertError(null, null, Collections.singletonList("nullValueForNotNull")),
            new String[] {"nullValueForNotNull"},
            new String[] {}),
        Arguments.of(
            createInsertError(
                Collections.emptyList(),
                Collections.singletonList("missingNotNull"),
                Collections.emptyList()),
            new String[] {"missingNotNull"},
            new String[] {}),
        Arguments.of(
            createInsertError(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList("nullValueForNotNull")),
            new String[] {"nullValueForNotNull"},
            new String[] {}));
  }

  private static InsertValidationResponse.InsertError createInsertError(
      List<String> extraColNames,
      List<String> missingNotNullColNames,
      List<String> nullValueForNotNullColNames) {
    InsertValidationResponse.InsertError insertError =
        new InsertValidationResponse.InsertError(new Object(), 1);
    insertError.setExtraColNames(extraColNames);
    insertError.setMissingNotNullColNames(missingNotNullColNames);
    insertError.setNullValueForNotNullColNames(nullValueForNotNullColNames);
    return insertError;
  }
}
