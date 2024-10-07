package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
   * @param schemaEvolutionEnabled whether schema evolution is enabled
   */
  public void validateTable(String tableName, String role, boolean schemaEvolutionEnabled) {
    List<DescribeTableRow> columns =
        snowflakeConnectionService
            .describeTable(tableName)
            .orElseThrow(() -> SnowflakeErrors.ERROR_0032.getException("table_not_found"));

    Optional<DescribeTableRow> metadata =
        columns.stream()
            .filter(c -> Objects.equals(c.getColumn(), TABLE_COLUMN_METADATA))
            .findFirst();

    metadata.ifPresent(
        m -> {
          // if metadata column exists it must be of type OBJECT(), if not exists we create on our
          // own this column
          if (!isOfStructuredObjectType(m)) {
            throw SnowflakeErrors.ERROR_0032.getException("invalid_metadata_type");
          }
        });

    if (schemaEvolutionEnabled) {
      validateSchemaEvolutionScenario(tableName, role);
    } else {
      validateNoSchemaEvolutionScenario(columns);
    }
  }

  private void validateSchemaEvolutionScenario(String tableName, String role) {
    if (!snowflakeConnectionService.hasSchemaEvolutionPermission(tableName, role)) {
      throw SnowflakeErrors.ERROR_0032.getException("schema_evolution_not_enabled");
    }
  }

  private static void validateNoSchemaEvolutionScenario(List<DescribeTableRow> columns) {
    DescribeTableRow recordContent =
        columns.stream()
            .filter(c -> Objects.equals(c.getColumn(), TABLE_COLUMN_CONTENT))
            .findFirst()
            .orElseThrow(
                () -> SnowflakeErrors.ERROR_0032.getException("record_content_column_not_found"));

    if (!isOfStructuredObjectType(recordContent)) {
      throw SnowflakeErrors.ERROR_0032.getException("invalid_record_content_type");
    }
  }

  private static boolean isOfStructuredObjectType(DescribeTableRow metadata) {
    return metadata.getType().startsWith(SF_STRUCTURED_OBJECT);
  }
}
