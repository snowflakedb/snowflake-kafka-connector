package com.snowflake.kafka.connector.internal.schematization;

import java.sql.Connection;

/** Check whether the user has the role privilege to do schema evolution on a table */
@FunctionalInterface
public interface SchemaEvolutionRoleValidator {
  boolean validateRole(Connection conn, String role, String tableName);
}
