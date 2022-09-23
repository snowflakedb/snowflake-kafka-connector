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
  public static void setConnectGlobalInstanceId(UUID id) {
    if (id != null && !id.toString().isEmpty() && !id.equals(EMPTY_UUID)) {
      kcGlobalInstanceId = id;
      META_LOGGER.info("Set Kafka Connect global instance id tag for logging: '{}'", kcGlobalInstanceId);
    } else {
      META_LOGGER.info("Given Kafka Connect global instance id was invalid (null or empty), continuing to log without" +
        " it");
      kcGlobalInstanceId = EMPTY_UUID;
    }
  }

  private Logger logger;
  private String loggerInstanceIdTag = "";

  public LoggerHandler() {
    this.logger = LoggerFactory.getLogger("test");
  }
  /**
   * Create and return a new logging handler
   *
   * @param name The class name passed for initializing the logger
   */
  public LoggerHandler(String name) {
    this.logger = LoggerFactory.getLogger(name);

    META_LOGGER.info(
      kcGlobalInstanceId.equals(EMPTY_UUID)
            ? Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' without a Kafka Connect global instance id.",
                name)
            : Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' with Kafka Connect global instance id: '{}'",
                name,
            kcGlobalInstanceId));
  }

  /**
   * Sets the loggerHandler's instance id tag
   *
   * Note: this should be called after the kc instance id has been set
   *
   * @param loggerTag The tag for this logger
   */
  public void setLoggerInstanceIdTag(String loggerTag) {
    if (loggerTag == null || loggerTag.isEmpty()) {
      this.logger.warn("Given logger tag '{}' is invalid (null or empty), continuing to log " +
          "without it", loggerTag);
      return;
    }

    this.loggerInstanceIdTag = loggerTag;
    this.logger.info("Given logger tag set to: '{}'", this.loggerInstanceIdTag);
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
    String tag = "";

    if (!kcGlobalInstanceId.equals(EMPTY_UUID)) {
      if (!this.loggerInstanceIdTag.isEmpty()) {
        tag = Utils.formatString("[KC:{}|{}] ", kcGlobalInstanceId, this.loggerInstanceIdTag);
      } else {
        tag = Utils.formatString("[KC:{}] ", kcGlobalInstanceId);
      }
    } else if (!this.loggerInstanceIdTag.isEmpty()){
      tag = Utils.formatString("[{}] ", this.loggerInstanceIdTag);
    }

    return Utils.formatLogMessage(tag + msg, vars);
  }
}