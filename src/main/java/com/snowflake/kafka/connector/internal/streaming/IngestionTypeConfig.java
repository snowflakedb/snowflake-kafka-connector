package com.snowflake.kafka.connector.internal.streaming;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public enum IngestionTypeConfig {
  SNOWPIPE("1.0"),

  // Ignore would filter out records which has null value, but a valid key.
  STREAMING_INGESTION("2.0"),
  ;

  final String version;

  IngestionTypeConfig(String version) {
    this.version = version;
  }

  public String getVersion() {
    return version;
  }

  private static final Map<String, IngestionTypeConfig> versionToIngestionType;

  static {
    versionToIngestionType = new HashMap<>();
    for (IngestionTypeConfig v : IngestionTypeConfig.values()) {
      versionToIngestionType.put(v.version, v);
    }
  }

  /* Validator to validate behavior.on.null.values which says whether kafka should keep null value records or ignore them while ingesting into snowflake table. */
  public static final ConfigDef.Validator VALIDATOR =
      new ConfigDef.Validator() {
        private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

        @Override
        public void ensureValid(String name, Object value) {
          if (value instanceof String) {
            value = ((String) value).toLowerCase(Locale.ROOT);
          }
          validator.ensureValid(name, value);
        }

        // Overridden here so that ConfigDef.toEnrichedRst shows possible values correctly
        @Override
        public String toString() {
          return validator.toString();
        }
      };

  // All valid enum values which returns the version numbers
  public static String[] names() {
    IngestionTypeConfig[] ingestionTypes = values();
    String[] result = new String[ingestionTypes.length];

    for (int i = 0; i < ingestionTypes.length; i++) {
      result[i] = ingestionTypes[i].getVersion();
    }

    return result;
  }

  public static IngestionTypeConfig getIngestionTypeFromVersion(String version) {
    return versionToIngestionType.get(version);
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
