package com.snowflake.kafka.connector.internal.streaming;

import java.util.Locale;
import org.apache.kafka.common.config.ConfigDef;

public enum IngestionTypeConfig {

  /* Default Way of ingestion */
  SNOWPIPE,

  /* Snowpipe streaming which doesnt convert records to intermediate files (from client perspective) */
  SNOWPIPE_STREAMING,
  ;

  /* Validator to validate behavior.on.null.values which says whether kafka should keep null value records or ignore them while ingesting into snowflake table. */
  public static final ConfigDef.Validator VALIDATOR =
      new ConfigDef.Validator() {
        private final ConfigDef.ValidString validator =
            ConfigDef.ValidString.in(IngestionTypeConfig.allIngestionTypes());

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

  // All valid enum values
  public static String[] allIngestionTypes() {
    IngestionTypeConfig[] configs = values();
    String[] result = new String[configs.length];

    for (int i = 0; i < configs.length; i++) {
      result[i] = configs[i].toString();
    }

    return result;
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
