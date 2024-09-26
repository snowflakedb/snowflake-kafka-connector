package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.List;
import java.util.Objects;

/** Performs validations of Iceberg table schema on the connector startup. */
public class IcebergTableSchemaValidator {

  private static final String SF_STRUCTURED_OBJECT = "OBJECT";

  private final SnowflakeConnectionService snowflakeConnectionService;

  public IcebergTableSchemaValidator(SnowflakeConnectionService snowflakeConnectionService) {
    this.snowflakeConnectionService = snowflakeConnectionService;
  }

  /**
   * Ensure that table exists and record_metadata column is of type OBJECT().
   *
   * @param tableName table to be validated
   * @param role role used for validation
   */
  public void validateTable(String tableName, String role) {
    List<DescribeTableRow> columns =
        snowflakeConnectionService
            .describeTable(tableName)
            .orElseThrow(() -> SnowflakeErrors.ERROR_0032.getException("table_not_found"));

    DescribeTableRow metadata =
        columns.stream()
            .filter(c -> Objects.equals(c.getColumn(), TABLE_COLUMN_METADATA))
            .findFirst()
            .orElseThrow(
                () -> SnowflakeErrors.ERROR_0032.getException("record_metadata_not_found"));

    if (!isOfStructuredObjectType(metadata)) {
      throw SnowflakeErrors.ERROR_0032.getException("invalid_metadata_type");
    }

    if (!snowflakeConnectionService.hasSchemaEvolutionPermission(tableName, role)) {
      throw SnowflakeErrors.ERROR_0032.getException("schema_evolution_not_enabled");
    }
  }

  private static boolean isOfStructuredObjectType(DescribeTableRow metadata) {
    return metadata.getType().startsWith(SF_STRUCTURED_OBJECT);
  }
}
