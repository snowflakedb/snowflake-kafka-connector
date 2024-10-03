package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import net.snowflake.ingest.streaming.InsertValidationResponse;

public class InsertErrorMapper {

  public SchemaEvolutionTargetItems mapToSchemaEvolutionItems(
      InsertValidationResponse.InsertError insertError, String tableName) {
    List<String> extraColNames = insertError.getExtraColNames();
    List<String> nonNullableColumns = insertError.getMissingNotNullColNames();
    List<String> nullValueForNotNullColNames = insertError.getNullValueForNotNullColNames();

    return new SchemaEvolutionTargetItems(
        tableName,
        joinNullableLists(nonNullableColumns, nullValueForNotNullColNames),
        extraColNames);
  }

  private List<String> joinNullableLists(List<String> list1, List<String> list2) {
    return Lists.newArrayList(
        Iterables.concat(
            Optional.ofNullable(list1).orElse(ImmutableList.of()),
            Optional.ofNullable(list2).orElse(ImmutableList.of())));
  }
}
