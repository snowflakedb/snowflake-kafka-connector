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
    kcGlobalInstanceIdTag =
        parseUuidIntoTag("KC", kcGlobalInstanceId, "Kafka Connect global");
  }

  /**
   * Create instance id tag from given descriptor and uuid
   *
   * <p>Note: empty string will be returned if the uuid or descriptor is null or empty
   *
   * @param descriptor The string to be prepended to in the tag
   * @param uuid The instance id uuid
   * @param logIdName Name of the tag for logging
   * @return A formatted instance id tag or empty striing
   */
  private static String parseUuidIntoTag(
      String descriptor, UUID uuid, String logIdName) {
    if (uuid == null || uuid.toString().isEmpty()) {
      META_LOGGER.warn(
          Utils.formatLogMessage(
              "Given {} instance id was invalid (null or empty), continuing to log without"
                  + " it"),
          logIdName);
      return "";
    }

    if (descriptor == null || descriptor.toString().isEmpty()) {
      META_LOGGER.warn(
          Utils.formatLogMessage(
              "Descriptor given for {} instance id was invalid (null or empty), continuing to log"
                  + " without it"),
          logIdName);
      return "";
    }

    if (descriptor.length() > 10) {
      META_LOGGER.info(
          Utils.formatLogMessage(
              "Given {} instance id descriptor '{}' is recommended to be below 10 characters",
              logIdName,
              descriptor));
    }

    META_LOGGER.info(
        Utils.formatLogMessage("Setting {} instance id to '{}'", logIdName, kcGlobalInstanceIdTag));

    return "[" + descriptor + ":" + uuid.toString() + "] ";
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
   * Create and return a new logging handler with logger specific instance id
   *
   * @param name The class name passed for initializing the logger
   * @param loggerInstanceId The instance id for this logger
   * @param loggerInstanceIdDescriptor The descriptor for this logger
   */
  public LoggerHandler(String name, UUID loggerInstanceId, String loggerInstanceIdDescriptor) {
    this(name);
    this.loggerInstanceIdTag =
        parseUuidIntoTag(loggerInstanceIdDescriptor, loggerInstanceId, "logger");
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
    String fullMsg = kcGlobalInstanceIdTag + this.loggerInstanceIdTag + msg;
    return Utils.formatLogMessage(fullMsg, vars);
  }
}
