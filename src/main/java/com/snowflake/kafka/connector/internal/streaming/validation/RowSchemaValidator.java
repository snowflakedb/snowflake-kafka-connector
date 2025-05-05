package com.snowflake.kafka.connector.internal.streaming.validation;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RowSchemaValidator {
  // Map of the column name to the column object, used for null/missing column check
  private static final ZoneId defaultTimezone = ZoneId.of("America/Los_Angeles");

  /**
   * Verify that the input row columns are all valid.
   *
   * <p>Checks that the columns, specified in the row, are present in the table and values for all
   * non-nullable columns are specified and different from null.
   *
   * @param row the input row
   * @param error the insert error that we return to the customer
   * @param rowIndex the index of the current row in the input batch
   */
  static void validateRowColumns(
      RowSchema rowSchema, Map<String, Object> row, PkgInsertError error, int rowIndex) {
    // Map of unquoted column name -> original column name
    Set<String> originalKeys = row.keySet();
    Map<String, String> inputColNamesMap = new HashMap<>();
    originalKeys.forEach(
        key -> inputColNamesMap.put(PkgLiteralQuoteUtils.unquoteColumnName(key), key));
    // Check for extra columns in the row
    List<String> extraCols = new ArrayList<>();
    for (String columnName : inputColNamesMap.keySet()) {
      if (!rowSchema.hasColumn(columnName)) {
        extraCols.add(inputColNamesMap.get(columnName));
      }
    }

    if (!extraCols.isEmpty()) {
      if (error != null) {
        error.setExtraColNames(extraCols);
      }
      throw new PkgSFException(
          ErrorCode.INVALID_FORMAT_ROW,
          "Extra columns: " + extraCols,
          String.format(
              "Columns not present in the table shouldn't be specified, rowIndex:%d", rowIndex));
    }

    // Check for missing columns in the row
    List<String> missingCols = new ArrayList<>();
    for (String columnName : rowSchema.getNonNullableFieldNames()) {
      if (!inputColNamesMap.containsKey(columnName)) {
        missingCols.add(rowSchema.get(columnName).columnMetadata.getName());
      }
    }

    if (!missingCols.isEmpty()) {
      if (error != null) {
        error.setMissingNotNullColNames(missingCols);
      }
      throw new PkgSFException(
          ErrorCode.INVALID_FORMAT_ROW,
          "Missing columns: " + missingCols,
          String.format(
              "Values for all non-nullable columns must be specified, rowIndex:%d", rowIndex));
    }

    // Check for null values in not-nullable columns in the row
    List<String> nullValueNotNullCols = new ArrayList<>();
    for (String columnName : rowSchema.getNonNullableFieldNames()) {
      if (inputColNamesMap.containsKey(columnName)
          && row.get(inputColNamesMap.get(columnName)) == null) {
        nullValueNotNullCols.add(rowSchema.get(columnName).columnMetadata.getName());
      }
    }

    if (!nullValueNotNullCols.isEmpty()) {
      if (error != null) {
        error.setNullValueForNotNullColNames(nullValueNotNullCols);
      }
      throw new PkgSFException(
          ErrorCode.INVALID_FORMAT_ROW,
          "Not-nullable columns with null values: " + nullValueNotNullCols,
          String.format(
              "Values for all non-nullable columns must not be null, rowIndex:%d", rowIndex));
    }
  }

  static void validateRowValues(
      RowSchema rowSchema, Map<String, Object> row, PkgInsertError error, long rowIndex) {

    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String columnName = PkgLiteralQuoteUtils.unquoteColumnName(key);
      PkgParquetColumn parquetColumn = rowSchema.get(columnName);
      ColumnMetadata column = parquetColumn.columnMetadata;
      if (rowSchema.isIceberg()) {
        PkgParquetValueParserIceberg.parseColumnValueToParquet(
            value,
            parquetColumn.type,
            rowSchema.getSubColumnFinder(),
            defaultTimezone,
            rowIndex,
            error);
      } else {
        PkgParquetValueParserSnowflake.parseColumnValueToParquet(
            value,
            column,
            parquetColumn.type.asPrimitiveType().getPrimitiveTypeName(),
            defaultTimezone,
            rowIndex,
            true);
      }
    }
  }
}
