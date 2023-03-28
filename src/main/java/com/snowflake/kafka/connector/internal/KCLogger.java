package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logger for Snowflake Sink Connector. Also attaches additional fields to the logs */
public class KCLogger {
  // static properties and methods
  private static final Logger META_LOGGER = LoggerFactory.getLogger(KCLogger.class.getName());
  private static final String EMPTY_ID = new UUID(0L, 0L).toString();

  private static String kcGlobalInstanceId = EMPTY_ID;

  /**
   * Sets the KC global instance id for all loggers.
   *
   * <p>This should only be called in start so that the entire kafka connector instance has the same
   * tag for logging
   *
   * <p>If an invalid id is given, continue to log without the id
   *
   * @param id UUID attached for every log
   */
  public static void setConnectGlobalInstanceId(UUID id) {
    setConnectGlobalInstanceId(id.toString());
  }

  /**
   * Sets the KC global instance id for all loggers.
   *
   * <p>This should only be called in start so that the entire kafka connector instance has the same
   * tag for logging
   *
   * <p>If an invalid id is given, continue to log without the id
   *
   * @param id String attached to every log
   */
  public static void setConnectGlobalInstanceId(String id) {
    if (id != null && !id.isEmpty() && !id.equals(EMPTY_ID)) {
      kcGlobalInstanceId = id;
      META_LOGGER.info(
          "Set Kafka Connect global instance id tag for logging: '{}'", kcGlobalInstanceId);
    } else {
      META_LOGGER.info(
          "Given Kafka Connect global instance id was invalid (null or empty), continuing to log"
              + " without it");
      kcGlobalInstanceId = EMPTY_ID;
    }
  }

  private Logger logger;
  private String loggerInstanceTag = "";

  /**
   * Create and return a new logging handler
   *
   * @param name The class name passed for initializing the logger
   */
  public KCLogger(String name) {
    this.logger = LoggerFactory.getLogger(name);

    META_LOGGER.info(
        kcGlobalInstanceId.equals(EMPTY_ID)
            ? Utils.formatLogMessage(
                "Created KCLogger for class: '{}' without a Kafka Connect global instance id.",
                name)
            : Utils.formatLogMessage(
                "Created KCLogger for class: '{}' with Kafka Connect global instance id: '{}'",
                name,
                kcGlobalInstanceId));
  }

  /**
   * Sets the KCLogger's instance tag.
   *
   * <p>Note: this should be called after the kc instance id has been set
   *
   * @param loggerTag The tag for this logger
   */
  public void setLoggerInstanceTag(String loggerTag) {
    if (loggerTag == null || loggerTag.isEmpty()) {
      this.logger.warn(
          "Given logger tag '{}' is invalid (null or empty), continuing to log " + "without it",
          loggerTag);
      return;
    }

    this.loggerInstanceTag = loggerTag;
    this.logger.info("Given logger tag set to: '{}'", this.loggerInstanceTag);
  }

  /** Clears the Logger's instance id tag */
  public void clearLoggerInstanceIdTag() {
    this.loggerInstanceTag = "";
  }

  /**
   * Logs an info level message
   *
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void info(String format, Object... vars) {
    if (this.logger.isInfoEnabled()) {
      this.logger.info(getFormattedMsg(format, vars));
    }
  }

  /**
   * Logs an trace level message
   *
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void trace(String format, Object... vars) {
    if (this.logger.isTraceEnabled()) {
      this.logger.trace(getFormattedMsg(format, vars));
    }
  }

  /**
   * Logs an debug level message
   *
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void debug(String format, Object... vars) {
    if (this.logger.isDebugEnabled()) {
      this.logger.debug(getFormattedMsg(format, vars));
    }
  }

  /**
   * Logs an warn level message
   *
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void warn(String format, Object... vars) {
    if (this.logger.isWarnEnabled()) {
      this.logger.warn(getFormattedMsg(format, vars));
    }
  }

  /**
   * Logs an error level message
   *
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void error(String format, Object... vars) {
    if (this.logger.isErrorEnabled()) {
      this.logger.error(getFormattedMsg(format, vars));
    }
  }

  /**
   * Format the message by attaching instance id tags and sending to Utils for final formatting
   *
   * @param msg The message format without variables that needs to be prepended with tags
   * @param vars The variables to insert into the format, these are passed directly to Utils
   * @return The fully formatted string to be logged
   */
  private String getFormattedMsg(String msg, Object... vars) {
    String tag = "";

    if (!kcGlobalInstanceId.equals(EMPTY_ID)) {
      if (!this.loggerInstanceTag.isEmpty()) {
        tag = Utils.formatString("[KC:{}|{}] ", kcGlobalInstanceId, this.loggerInstanceTag);
      } else {
        tag = Utils.formatString("[KC:{}] ", kcGlobalInstanceId);
      }
    } else if (!this.loggerInstanceTag.isEmpty()) {
      tag = Utils.formatString("[{}] ", this.loggerInstanceTag);
    }

    return Utils.formatLogMessage(tag + msg, vars);
  }
}
