package com.snowflake.kafka.connector.internal;

// The purpose of this class is for non-static inheritors to easily log information
public abstract class EnableLogging {
  private final LoggerHandler logger = new LoggerHandler(this.getClass().getName());

  protected void LOG_TRACE_MSG(String format) {
    this.logger.trace(format);
  }

  protected void LOG_INFO_MSG(String format) {
    this.logger.info(format);
  }

  protected void LOG_WARN_MSG(String format) {
    this.logger.warn(format);
  }

  protected void LOG_DEBUG_MSG(String format) {
    this.logger.debug(format);
  }

  protected void LOG_ERROR_MSG(String format) {
    this.logger.error(format);
  }

  protected void LOG_TRACE_MSG(String format, Object... vars) {
    this.logger.trace(format, vars);
  }

  protected void LOG_INFO_MSG(String format, Object... vars) {
    this.logger.info(format, vars);
  }

  protected void LOG_WARN_MSG(String format, Object... vars) {
    this.logger.warn(format, vars);
  }

  protected void LOG_DEBUG_MSG(String format, Object... vars) {
    this.logger.debug(format, vars);
  }

  protected void LOG_ERROR_MSG(String format, Object... vars) {
    this.logger.error(format, vars);
  }
}
