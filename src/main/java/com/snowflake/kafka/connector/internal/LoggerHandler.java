package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/** Attaches additional fields to the logs */
public class LoggerHandler {
  // static properties and methods
  private static final Logger META_LOGGER = LoggerFactory.getLogger(LoggerHandler.class.getName());
  private static final UUID EMPTY_UUID = new UUID(0L, 0L);

  private static UUID kcGlobalInstanceId = new UUID(0L,0L);
  private static String kcGlobalInstanceIdTag = "";

  /**
   * Sets the KC global instance id for all loggers.
   *
   * <p>This should only be called in start so that the entire kafka connector instance has the same
   * correlationId logging.
   *
   * <p>If an invalid id is given, continue to log without the id
   *
   * @param id UUID attached for every log
   */
  public static void setKcGlobalInstanceId(UUID id) {
    if (id != null && !id.toString().isEmpty() && !id.equals(EMPTY_UUID)) {
      kcGlobalInstanceId = id;
      kcGlobalInstanceIdTag = Utils.formatString("[KC:{}]", id);
      META_LOGGER.info("Set Kafka Connect global instance id tag for logging: '{}'", kcGlobalInstanceIdTag);
    } else {
      META_LOGGER.info("Given Kafka Connect global instance id was invalid (null or empty), continuing to log without" +
        " it");
      kcGlobalInstanceId = EMPTY_UUID;
      kcGlobalInstanceIdTag = "";
    }
  }

  private Logger logger;
  private String loggerInstanceIdTag = "";

  /**
   * Create and return a new logging handler
   *
   * @param name The class name passed for initializing the logger
   */
  public LoggerHandler(String name) {
    this.logger = LoggerFactory.getLogger(name);

    META_LOGGER.info(
        kcGlobalInstanceIdTag.equals("")
            ? Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' without a Kafka Connect global instance id.",
                name)
            : Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' with Kafka Connect global instance id: '{}'",
                name,
                kcGlobalInstanceIdTag));
  }

  /**
   * Sets the loggerHandler's instance id tag. If kc instance id exists, then it will append the tag to the end,
   * otherwise it will use the fallback id.
   *
   * Note: this should be called after the kc instance id has been set
   *
   * @param loggerTag The tag for this logger
   * @param fallbackLoggerInstanceId The instance id for this logger (if kc instance id doesnt exist)
   */
  public void setLoggerInstanceIdTag(String loggerTag, UUID fallbackLoggerInstanceId) {
    if (loggerTag == null || loggerTag.isEmpty()
          || fallbackLoggerInstanceId == null || fallbackLoggerInstanceId.equals(EMPTY_UUID)) {
      this.logger.warn("Given logger tag '{}' or fallback id '{}' is invalid (null or empty), continuing to log " +
          "without it", loggerTag,
        fallbackLoggerInstanceId);
      return;
    }

    if (kcGlobalInstanceId != EMPTY_UUID) {
      this.loggerInstanceIdTag = Utils.formatString("[KC:{}|{}]", kcGlobalInstanceId, loggerTag);
    } else {
      this.loggerInstanceIdTag = Utils.formatString("[{}:{}]", loggerTag, fallbackLoggerInstanceId);
    }

    this.logger.info("Given logger tag set to: '{}'", loggerInstanceIdTag);
  }

  /** Clears the loggerHandler's instance id tag */
  public void clearLoggerInstanceIdTag() {
    this.loggerInstanceIdTag = "";
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
    String tag = !this.loggerInstanceIdTag.isEmpty() ? this.loggerInstanceIdTag + " "
      : !kcGlobalInstanceIdTag.isEmpty() ? kcGlobalInstanceIdTag + " "
        : "";

    return Utils.formatLogMessage(tag + msg, vars);
  }
}
