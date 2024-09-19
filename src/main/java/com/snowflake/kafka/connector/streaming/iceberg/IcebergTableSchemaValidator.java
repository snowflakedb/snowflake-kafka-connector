package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;

/**
 * Performs validations of Iceberg table schema on the connector startup.
 **/
public class IcebergTableSchemaValidator {

    private final SnowflakeConnectionService snowflakeConnectionService;

    public IcebergTableSchemaValidator(SnowflakeConnectionService snowflakeConnectionService) {
        this.snowflakeConnectionService = snowflakeConnectionService;
    }

    /**
     * Ensure that table exists and record_metadata column is of type OBJECT().
     * TODO SNOW-1658914 - write a test for table with record_metadata schema altered by the connector
     */
    public void validateTable(String tableName, String role) {
        if (!snowflakeConnectionService.tableExist(tableName)) {
            throw new RuntimeException("TODO");
        }

        if (!snowflakeConnectionService.hasSchemaEvolutionPermission(tableName, role)) {
            throw new RuntimeException("TODO");
        }

        // TODO - call describe table and analyze record_metadata schema
    }
}
