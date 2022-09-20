package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Attaches additional fields to the logs
 */
public class LoggerHandler {
  // static properties and methods
  private static final UUID CORRELATION_ID_EMPTY = null;
  private static final Logger META_LOGGER = LoggerFactory.getLogger(LoggerHandler.class.getName());
  private static UUID loggerCorrelationId = CORRELATION_ID_EMPTY;

  /**
   * Sets the correlationId for all loggers. This should only be called in start so that the entire kafka connector
   * instance has the same correlationId logging
   * @param correlationId UUID attached for every log
   */
  public static void setCorrelationUuid(UUID correlationId) {
    loggerCorrelationId = correlationId;

    if (correlationId == null) {
      META_LOGGER.warn(
          Utils.formatLogMessage(
              "Given correlationId was null, continuing to log without a correlationId"));
    } else {
      META_LOGGER.info(
          Utils.formatLogMessage(
              "Setting correlationId for all logging in this instance of Snowflake Kafka"
                  + " Connector to '{}'",
              correlationId.toString()));
    }
  }

  private Logger logger;

  /**
   * Create and return a new logging handler
   * @param name The class name passed for initializing the logger
   */
  public LoggerHandler(String name) {
    this.logger = LoggerFactory.getLogger(name);

    if (isCorrelationIdValid()) {
      META_LOGGER.info(
          Utils.formatLogMessage(
              "Created loggerHandler for class: '{}' with correlationId: " + "'{}'",
              name,
              loggerCorrelationId.toString()));
    } else {
      META_LOGGER.info(
          Utils.formatLogMessage(
              "Created loggerHandler for class: '{}' without a correlationId.", name));
    }
  }

  /**
   * Logs an info level message
   * @param msg The message to be logged
   */
  public void info(String msg) {
    if (this.logger.isInfoEnabled()) {
      this.logger.info(getFormattedMsg(msg));
    }
  }

  /**
   * Logs a trace level message
   * @param msg The message to be logged
   */
  public void trace(String msg) {
    if (this.logger.isTraceEnabled()) {
      this.logger.trace(getFormattedMsg(msg));
    }
  }

  /**
   * Logs a debug level message
   * @param msg The message to be logged
   */
  public void debug(String msg) {
    if (this.logger.isDebugEnabled()) {
      this.logger.debug(getFormattedMsg(msg));
    }
  }

  /**
   * Logs a warn level message
   * @param msg The message to be logged
   */
  public void warn(String msg) {
    if (this.logger.isWarnEnabled()) {
      this.logger.warn(getFormattedMsg(msg));
    }
  }

  /**
   * Logs an error level message
   * @param msg The message to be logged
   */
  public void error(String msg) {
    if (this.logger.isErrorEnabled()) {
      this.logger.error(getFormattedMsg(msg));
    }
  }

  /**
   * Logs an info level message
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
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void error(String format, Object... vars) {
    if (this.logger.isErrorEnabled()) {
      this.logger.error(getFormattedMsg(format, vars));
    }
  }

  // format correctly and add correlationId tag if exists

  /**
   * Format the message correctly by attaching correlationId if needed and passing to Utils for more formatting
   * @param msg The message that needs to be prepended with tags
   * @return The fully formatted string to be logged
   */
  private String getFormattedMsg(String msg) {
    return Utils.formatLogMessage(getCorrelationIdStr() + msg);
  }

  /**
   * Format the message correctly by injecting the variables, attaching correlationId if needed, and passing to Utils
   * for more formatting
   * @param msg The message format without variables that needs to be prepended with tags
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   * @return The fully formatted string to be logged
   */
  private String getFormattedMsg(String msg, Object... vars) {
    return Utils.formatLogMessage(getCorrelationIdStr() + msg, vars);
  }

  /**
   * Check if the correlationId is valid
   * @return true if the correlationId is valid, false otherwise
   */
  private static boolean isCorrelationIdValid() {
    return loggerCorrelationId != CORRELATION_ID_EMPTY
        && !loggerCorrelationId.toString().isEmpty();
  }

  /**
   * Creates the correlationId tag
   * @return The correlationId tag
   */
  private static String getCorrelationIdStr() {
    return isCorrelationIdValid() ? "[" + loggerCorrelationId.toString() + "] " : "";
  }
}
