package com.snowflake.kafka.connector.internal;


import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class LoggerHandler {
  // static properties and methods
  private static final String CORRELATION_ID_LOG_FORMAT = " - with CorrelationId: ";
  private static final UUID CORRELATION_ID_EMPTY = null;
  private static final Logger META_LOGGER = LoggerFactory.getLogger(LoggerHandler.class.getName());
  private static UUID LOGGER_CORRELATION_ID = CORRELATION_ID_EMPTY;

  // should only be called on start
  public static void setCorrelationUuid(UUID correlationId) {
    LOGGER_CORRELATION_ID = correlationId;
    META_LOGGER.info(Utils.formatLogMessage("Setting correlationId for all logging in this instance of Snowflake Kafka" +
        " Connector to '{}'",
      correlationId.toString()));
  }

  private Logger logger;

  // create logger handler without changing correlationId
  public LoggerHandler(String name) {
    this.logger = LoggerFactory.getLogger(name);

    if (isCorrelationIdValid()) {
      META_LOGGER.info(Utils.formatLogMessage("Created loggerHandler for class: '{}' with correlationId: " +
          "'{}'",
        name, LOGGER_CORRELATION_ID.toString()));
    } else {
      META_LOGGER.info(Utils.formatLogMessage("Created loggerHandler for class: '{}' without a correlationId.",
        name));
    }
  }

  // only message
  public void info(String msg) {
    if (this.logger.isInfoEnabled()) {
      this.logger.info(getFormattedMsg(msg));
    }
  }

  public void trace(String msg) {
    if (this.logger.isTraceEnabled()) {
      this.logger.trace(getFormattedMsg(msg));
    }
  }

  public void debug(String msg) {
    if (this.logger.isDebugEnabled()) {
      this.logger.debug(getFormattedMsg(msg));
    }
  }

  public void warn(String msg) {
    if (this.logger.isWarnEnabled()) {
      this.logger.warn(getFormattedMsg(msg));
    }
  }

  public void error(String msg) {
    if (this.logger.isErrorEnabled()) {
      this.logger.error(getFormattedMsg(msg));
    }
  }

  // format and variables
  public void info(String format, Object... vars) {
    if (this.logger.isInfoEnabled()) {
      this.logger.info(getFormattedMsg(format, vars));
    }
  }

  public void trace(String format, Object... vars) {
    if (this.logger.isTraceEnabled()) {
      this.logger.trace(getFormattedMsg(format, vars));
    }
  }

  public void debug(String format, Object... vars) {
    if (this.logger.isDebugEnabled()) {
      this.logger.debug(getFormattedMsg(format, vars));
    }
  }

  public void warn(String format, Object... vars) {
    if (this.logger.isWarnEnabled()) {
      this.logger.warn(getFormattedMsg(format, vars));
    }
  }

  public void error(String format, Object... vars) {
    if (this.logger.isErrorEnabled()) {
      this.logger.error(getFormattedMsg(format, vars));
    }
  }

  // format correctly and add correlationId tag if exists
  private String getFormattedMsg(String msg) {
    return isCorrelationIdValid() ?
      Utils.formatLogMessage(msg) + CORRELATION_ID_LOG_FORMAT + LOGGER_CORRELATION_ID :
      Utils.formatLogMessage(msg);
  }

  private String getFormattedMsg(String msg, Object... vars) {
    return isCorrelationIdValid() ?
      Utils.formatLogMessage(msg, vars) + CORRELATION_ID_LOG_FORMAT + LOGGER_CORRELATION_ID :
      Utils.formatLogMessage(msg);
  }

  private static boolean isCorrelationIdValid() {
    return LOGGER_CORRELATION_ID != null
      && !LOGGER_CORRELATION_ID.toString().isEmpty()
      && LOGGER_CORRELATION_ID != CORRELATION_ID_EMPTY;
  }
}
