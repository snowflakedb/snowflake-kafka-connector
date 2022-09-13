package com.snowflake.kafka.connector.internal;


import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class LoggerHandler {
  private final String CORRELATIONID_TAG = " - with CorrelationId: ";
  private String loggerCorrelationIdStr;
  private Logger logger;

  // create logger handler without correlationId
  public LoggerHandler(String name) {
    this.logger = LoggerFactory.getLogger(name);

    this.logger.info(Utils.formatLogMessage("Created loggerHandler for class: '{}' without a correlationId", name));
  }

  // return logger with correlationId
  public LoggerHandler(UUID loggerCorrelationId, String name) {
    this.loggerCorrelationIdStr = loggerCorrelationId.toString();
    this.logger = LoggerFactory.getLogger(name);

    this.logger.info(Utils.formatLogMessage("Created loggerHandler for class: '{}' with correlationId", name,
      this.loggerCorrelationIdStr));
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
    return this.loggerCorrelationIdStr == null || this.loggerCorrelationIdStr.isEmpty() ?
      Utils.formatLogMessage(msg) :
      Utils.formatLogMessage(msg) + CORRELATIONID_TAG + this.loggerCorrelationIdStr;
  }

  private String getFormattedMsg(String msg, Object... vars) {
    return this.loggerCorrelationIdStr == null || this.loggerCorrelationIdStr.isEmpty() ?
      Utils.formatLogMessage(msg) :
      Utils.formatLogMessage(msg, vars) + CORRELATIONID_TAG + this.loggerCorrelationIdStr;
  }
}
