package com.snowflake.kafka.connector.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Class which validates key value pairs in the format <key-1>:<value-1>,<key-2>:<value-2>
 *
 * <p>It doesn't validate the type of values, only making sure the format is correct.
 */
class CommaSeparatedKeyValueValidator implements ConfigDef.Validator {
  public CommaSeparatedKeyValueValidator() {}

  public void ensureValid(String name, Object value) {
    String s = (String) value;
    // Validate the comma-separated key-value pairs string
    if (s != null && !s.isEmpty() && !isValidCommaSeparatedKeyValueString(s)) {
      throw new ConfigException(name, value, "Format: <key-1>:<value-1>,<key-2>:<value-2>,...");
    }
  }

  private boolean isValidCommaSeparatedKeyValueString(String input) {
    // Split the input string by commas
    String[] pairs = input.split(",");
    for (String pair : pairs) {
      // Trim the pair to remove leading and trailing whitespaces
      pair = pair.trim();
      // Split each pair by colon
      String[] keyValue = pair.split(":");
      // Check if the pair has exactly two elements after trimming
      if (keyValue.length != 2) {
        return false;
      }
      // Check if the key or value is empty after trimming
      if (keyValue[0].trim().isEmpty() || keyValue[1].trim().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public String toString() {
    return "Comma-separated key-value pairs format: <key-1>:<value-1>,<key-2>:<value-2>,...";
  }
}
