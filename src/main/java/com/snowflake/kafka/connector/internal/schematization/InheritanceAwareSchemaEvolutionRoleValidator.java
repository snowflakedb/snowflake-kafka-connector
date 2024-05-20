package com.snowflake.kafka.connector.internal.schematization;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.SchematizationUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An implementation validates if a given role or any nested roles has privilege to perform schema
 * evolution.
 *
 * <p>Note that the implementation is limited to only a single level of role inheritance and was
 * created to fix SNOW-1417197. Considering multiple levels of role inheritance would be more
 * complicated because of e.g. possible cycles and is not worth additional complexity.
 */
public class InheritanceAwareSchemaEvolutionRoleValidator implements SchemaEvolutionRoleValidator {

  private static final String SHOW_GRANTS_QUERY = "show grants on table identifier(?)";
  private static final Set<String> SCHEMA_EVOLUTION_ALLOWED_PRIVILEGE_LIST =
      new HashSet<>(Arrays.asList("EVOLVE SCHEMA", "ALL", "OWNERSHIP"));

  @Override
  public boolean validateRole(Connection conn, String role, String tableName) {
    List<String> rolesToCheck = getRolesToCheckSchematizationPermissions(conn, role);
    try {
      PreparedStatement stmt = conn.prepareStatement(SHOW_GRANTS_QUERY);
      stmt.setString(1, tableName);
      ResultSet result = stmt.executeQuery();
      while (result.next()) {
        if (!rolesToCheck.contains(result.getString("grantee_name"))) {
          continue;
        }
        if (SCHEMA_EVOLUTION_ALLOWED_PRIVILEGE_LIST.contains(
            result.getString("privilege").toUpperCase())) {
          return true;
        }
      }
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2017.getException(e);
    }
    return false;
  }

  /** Return formatted role and inherited roles */
  private static List<String> getRolesToCheckSchematizationPermissions(
      Connection conn, String role) {
    String formattedRole = SchematizationUtils.formatName(role);

    List<String> ret = new ArrayList<>();
    ret.add(formattedRole);

    // TODO - validate that granted_on and granted_to equals to ROLE
    // extract to separate class and add basic tests with MockResultSet
    // add e2e test - it is better to have duplicated test than no test at all
    String query = "show grants to role " + role;
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet result = stmt.executeQuery();
      while (result.next()) {
        if (result.getString("granted_on").equals("ROLE")
            && result.getString("granted_to").equals("ROLE")) {
          ret.add(result.getString("name"));
        }
      }
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2017.getException(e);
    }
    return ret;
  }
}
