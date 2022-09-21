package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/** Attaches additional fields to the logs */
public class LoggerHandler {
  // static properties and methods
  private static final String EMPTY_ID_TAG = "";
  private static final Logger META_LOGGER = LoggerFactory.getLogger(LoggerHandler.class.getName());
  private static String kcGlobalInstanceIdTag = EMPTY_ID_TAG;

  /**
   * Sets the KC global instance id for all loggers.
   *
   * <p>This should only be called in start so that the entire kafka connector instance has the same
   * correlationId logging.
   *
   * <p>If an invalid id is given, continue to log without the id
   *
   * @param kcGlobalInstanceId UUID attached for every log
   */
  public static void setKcGlobalInstanceId(UUID kcGlobalInstanceId) {
    if (isUuidValid(kcGlobalInstanceId)) {
      kcGlobalInstanceIdTag = getIdTagStr(kcGlobalInstanceId);

      META_LOGGER.info(
          Utils.formatLogMessage(
              "Set Kafka Connect global instance id for all logging to '{}'",
              kcGlobalInstanceIdTag));
    } else {
      META_LOGGER.warn(
          Utils.formatLogMessage(
              "Given Kafka Connect global instance id was invalid (null or empty), continuing to log without"
                  + " it"));
    }
  }

  private Logger logger;

  /**
   * Create and return a new logging handler
   *
   * @param name The class name passed for initializing the logger
   */
  public LoggerHandler(String name) {
    this.logger = LoggerFactory.getLogger(name);

    META_LOGGER.info(
        kcGlobalInstanceIdTag.equals(EMPTY_ID_TAG)
            ? Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' without a Kafka Connect global instance id.",
                name)
            : Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' with Kafka Connect global instance id: '{}'",
                name,
                kcGlobalInstanceIdTag));
  }

  /**
   * Logs an info level message
   *
   * @param msg The message to be logged
   */
  public void info(String msg) {
    if (this.logger.isInfoEnabled()) {
      this.logger.info(getFormattedMsg(msg));
    }
  }

  /**
   * Logs a trace level message
   *
   * @param msg The message to be logged
   */
  public void trace(String msg) {
    if (this.logger.isTraceEnabled()) {
      this.logger.trace(getFormattedMsg(msg));
    }
  }

  /**
   * Logs a debug level message
   *
   * @param msg The message to be logged
   */
  public void debug(String msg) {
    if (this.logger.isDebugEnabled()) {
      this.logger.debug(getFormattedMsg(msg));
    }
  }

  /**
   * Logs a warn level message
   *
   * @param msg The message to be logged
   */
  public void warn(String msg) {
    if (this.logger.isWarnEnabled()) {
      this.logger.warn(getFormattedMsg(msg));
    }
  }

  /**
   * Logs an error level message
   *
   * @param msg The message to be logged
   */
  public void error(String msg) {
    if (this.logger.isErrorEnabled()) {
      this.logger.error(getFormattedMsg(msg));
    }
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
    String fullMsg = kcGlobalInstanceIdTag + msg;

    if (vars == null) {
      return Utils.formatLogMessage(fullMsg);
    }
    return Utils.formatLogMessage(fullMsg, vars);
  }

  /**
   * Check if the uuid is valid
   *
   * @param id The given uuid
   * @return true if the uuid is not null and not empty, false otherwise
   */
  private static boolean isUuidValid(UUID id) {
    return id != null && !id.toString().isEmpty();
  }

  /**
   * Creates the id tag
   *
   * @param id The given id
   * @return The id tag
   */
  private static String getIdTagStr(UUID id) {
    return "[" + id.toString() + "] ";
  }

  /**
   * Creates the id tag prepended with the descriptor
   *
   * @param id The given id
   * @param descriptor The string prepended before the id
   * @return The id tag
   */
  private static String getIdTagStr(UUID id, String descriptor) {
    return "[" + descriptor + ":" + id.toString() + "]";
  }
}
