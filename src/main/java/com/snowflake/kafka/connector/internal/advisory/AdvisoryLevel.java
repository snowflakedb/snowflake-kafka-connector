package com.snowflake.kafka.connector.internal.advisory;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Locale;

/** Maps a GS-supplied advisory level string to a KCLogger call. Unknown/null defaults to WARN. */
public enum AdvisoryLevel {
  INFO,
  WARN,
  ERROR;

  /**
   * Resolves a raw level string (e.g. "INFO", "warn", "Error") to the corresponding enum constant.
   * Returns {@link #WARN} for null or unrecognised values.
   */
  public static AdvisoryLevel fromString(String level) {
    if (level == null) {
      return WARN;
    }
    switch (level.toUpperCase(Locale.ROOT)) {
      case "INFO":
        return INFO;
      case "ERROR":
        return ERROR;
      case "WARN":
        return WARN;
      default:
        return WARN;
    }
  }

  /** Logs the given text through {@code logger} at this level. */
  public void log(KCLogger logger, String text) {
    switch (this) {
      case INFO:
        logger.info(text);
        break;
      case ERROR:
        logger.error(text);
        break;
      case WARN:
      default:
        logger.warn(text);
        break;
    }
  }
}
