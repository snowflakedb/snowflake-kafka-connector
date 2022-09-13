package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnector;

import java.util.UUID;

public class Logging {
  private LoggerHandler logger;

  public Logging() {
    SnowflakeSinkConnector.loggerHandlerFactory.getLogger(this.getClass().getName());
  }
  public void logTrace(String format) {
    this.logger.trace(format);
  }

  public void logInfo(String format) {
    this.logger.info(format);
  }

  public void logWarn(String format) {
    this.logger.warn(format);
  }

  public void logDebug(String format) {
    this.logger.debug(format);
  }

  public void logError(String format) {
    this.logger.error(format);
  }

  public void logTrace(String format, Object... vars) {
    this.logger.trace(format, vars);
  }

  public void logInfo(String format, Object... vars) {
    this.logger.info(format, vars);
  }

  public void logWarn(String format, Object... vars) {
    this.logger.warn(format, vars);
  }

  public void logDebug(String format, Object... vars) {
    this.logger.debug(format, vars);
  }

  public void logError(String format, Object... vars) {
    this.logger.error(format, vars);
  }
}
