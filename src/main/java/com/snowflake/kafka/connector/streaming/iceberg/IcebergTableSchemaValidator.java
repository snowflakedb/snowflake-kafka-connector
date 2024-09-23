package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;

/** Performs validations of Iceberg table schema on the connector startup. */
public class IcebergTableSchemaValidator {

  private final SnowflakeConnectionService snowflakeConnectionService;

  public IcebergTableSchemaValidator(SnowflakeConnectionService snowflakeConnectionService) {
    this.snowflakeConnectionService = snowflakeConnectionService;
  }

  /**
   * Ensure that table exists and record_metadata column is of type OBJECT().
   *
   * <p>TODO SNOW-1658914 - write a test for table with record_metadata schema altered by the
   * connector
   */
  public void validateTable(String tableName, String role) {
    // TODO - plug into connector startup
    if (!snowflakeConnectionService.tableExist(tableName)) {
      throw SnowflakeErrors.ERROR_0032.getException("table_not_found");
    }

    if (!snowflakeConnectionService.hasSchemaEvolutionPermission(tableName, role)) {
      throw SnowflakeErrors.ERROR_0032.getException("schema_evolution_not_enabled");
    }

    // TODO - call describe table and analyze record_metadata schema
  }
}
