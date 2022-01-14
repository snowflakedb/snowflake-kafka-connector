package com.snowflake.kafka.connector.internal.streaming;

import java.util.Locale;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Enum representing the allowed values for config {@link
 * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#INGESTION_METHOD_OPT}
 */
public enum IngestionMethodConfig {

  /* Default Way of ingestion */
  SNOWPIPE,

  /* Snowpipe streaming which doesnt convert records to intermediate files (from client perspective) */
  SNOWPIPE_STREAMING,
  ;

  /* Validator to validate snowflake.ingestion.method values */
  public static final ConfigDef.Validator VALIDATOR =
      new ConfigDef.Validator() {
        private final ConfigDef.ValidString validator =
            ConfigDef.ValidString.in(IngestionMethodConfig.allIngestionTypes());

        @Override
        public void ensureValid(String name, Object value) {
          if (value instanceof String) {
            value = ((String) value).toLowerCase(Locale.ROOT);
          }
          validator.ensureValid(name, value);
        }

        @Override
        public String toString() {
          return validator.toString();
        }
      };

  // All valid enum values
  public static String[] allIngestionTypes() {
    IngestionMethodConfig[] configs = values();
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
