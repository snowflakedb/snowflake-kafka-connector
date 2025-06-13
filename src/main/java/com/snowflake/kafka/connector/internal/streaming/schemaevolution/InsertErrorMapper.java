package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import static com.snowflake.kafka.connector.Utils.joinNullableLists;

import java.util.List;
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
}
