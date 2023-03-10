package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Attaches additional fields to the logs */
public class LoggerHandler {
  // static properties and methods
  private static final Logger META_LOGGER = LoggerFactory.getLogger(LoggerHandler.class.getName());

  // [KC:instanceid] where instanceid is a hash of a random uuid and the current time
  private static final String KC_GLOBAL_INSTANCEID_FORMAT = "[KC:{}]";
  // [TASK:taskId.creationtimestamp]
  // Example: [TASK:0.1678386676] indicates task 0 was started at 1678386676
  private static final String TASK_INSTANCE_TAG_FORMAT = "[TASK:{}.{}]";

  private static String kcGlobalInstanceId = "";

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
  public static void setKcGlobalInstanceId(String id) {
    if (id != null && !id.isEmpty()) {
      kcGlobalInstanceId = id;
      META_LOGGER.info(
          "Set Kafka Connect global instance id tag for logging: '{}'", kcGlobalInstanceId);
    } else {
      META_LOGGER.warn(
          "Given Kafka Connect global instance id was invalid (null or empty), continuing to log"
              + " without it");
      kcGlobalInstanceId = "";
    }
  }

  /**
   * Returns the instance id as the hashcode of the kc start time
   *
   * @param startTime the start time
   * @return the formatted instance id
   */
  public static String getFormattedKcGlobalInstanceId(long startTime) {
    return Utils.formatString(KC_GLOBAL_INSTANCEID_FORMAT, Math.abs(("" + startTime).hashCode()));
  }

  /**
   * Returns a formatted task logging tag as the taskid with a hash of the task start time
   *
   * @param taskId the task id
   * @param startTime the task start time
   * @return the formatted task logging tag
   */
  public static String getFormattedTaskLoggingTag(String taskId, long startTime) {
    return Utils.formatString(
        TASK_INSTANCE_TAG_FORMAT, taskId, Math.abs(("" + startTime).hashCode()));
  }

  private Logger logger;
  private String loggerInstanceTag = "";

  /**
   * Create and return a new logging handler
   *
   * @param name The class name passed for initializing the logger
   */
  public LoggerHandler(String name) {
    this.logger = LoggerFactory.getLogger(name);

    META_LOGGER.trace(
        kcGlobalInstanceId.isEmpty()
            ? Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' without a Kafka Connect global instance id.",
                name)
            : Utils.formatLogMessage(
                "Created loggerHandler for class: '{}' with Kafka Connect global instance id: '{}'",
                name,
                kcGlobalInstanceId));
  }

  /**
   * Sets the loggerHandler's instance tag.
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
    this.logger.debug("Given logger tag set to: '{}'", this.loggerInstanceTag);
  }

  /** Clears the loggerHandler's instance id tag */
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

    // instance id and tag should be empty if uninitialized
    if (!kcGlobalInstanceId.equals("")) {
      tag += kcGlobalInstanceId + " ";
    }

    if (!this.loggerInstanceTag.equals("")) {
      tag += this.loggerInstanceTag + " ";
    }

    return Utils.formatLogMessage(tag + msg, vars);
  }
}
