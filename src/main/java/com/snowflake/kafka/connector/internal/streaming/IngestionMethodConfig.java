package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Enum representing the allowed values for config {@link
 * SnowflakeSinkConnectorConfig#INGESTION_METHOD_OPT}
 *
 * <p>NOTE: Please do not change ordering of this Enums, please append to the end.
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

  /**
   * Returns the Ingestion Method found in User Configuration for Snowflake Kafka Connector.
   *
   * <p>Default is always {@link IngestionMethodConfig#SNOWPIPE} unless an invalid value is passed
   * which results in exception.
   */
  public static IngestionMethodConfig determineIngestionMethod(Map<String, String> inputConf) {
    if (inputConf == null
        || inputConf.isEmpty()
        || !inputConf.containsKey(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT)) {
      return IngestionMethodConfig.SNOWPIPE;
    } else {
      try {
        return IngestionMethodConfig.valueOf(
            inputConf
                .get(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT)
                .toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException ex) {
        throw ex;
      }
    }
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
