package com.snowflake.kafka.connector.internal.streaming;

import java.util.Locale;
import net.snowflake.ingest.utils.Constants;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Enum representing the allowed values for config {@link
 * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#SNOWPIPE_STREAMING_FILE_TYPE}
 *
 * <p>Note: Do not use ARROW format unless absolutely needed, contact the support team if needed be.
 */
public enum SnowpipeStreamingFileType {

  /* Arrow file format corresponds to bdec version 1, use only as a fallback when PARQUET doesnt work */
  ARROW(Constants.BdecVersion.ONE),

  /* Default BDEC file format PARQUET */
  PARQUET(Constants.BdecVersion.THREE),
  ;

  /** Bdec version defined in Ingest SDK */
  private final Constants.BdecVersion bdecVersion;

  /**
   * @return the bdec version associated with the file type. BDEC version later is passed in
   *     Streaming Client generation.
   */
  public Constants.BdecVersion getBdecVersion() {
    return bdecVersion;
  }

  /**
   * Validator to validate {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#SNOWPIPE_STREAMING_FILE_TYPE}
   */
  public static final ConfigDef.Validator VALIDATOR =
      new ConfigDef.Validator() {
        private final ConfigDef.ValidString validator =
            ConfigDef.ValidString.in(SnowpipeStreamingFileType.allSnowpipeStreamingFileTypes());

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

  SnowpipeStreamingFileType(Constants.BdecVersion bdecVersion) {
    this.bdecVersion = bdecVersion;
  }

  // All valid enum values
  public static String[] allSnowpipeStreamingFileTypes() {
    SnowpipeStreamingFileType[] configs = values();
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
