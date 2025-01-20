package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Logger for Snowflake Sink Connector. Attaches MDC's connector context if available */
public class KCLogger {
  public static final String MDC_CONN_CTX_KEY = "connector.context";
  private static boolean prependMdcContext;
  private static final Logger META_LOGGER = LoggerFactory.getLogger(KCLogger.class.getName());
  private Logger logger;

  /**
   * Enable or disables the MDC context. Only available for apache kafka versions after 2.3.0.
   * https://cwiki.apache.org/confluence/display/KAFKA/KIP-449%3A+Add+connector+contexts+to+Connect+worker+logs
   *
   * @param shouldPrependMdcContext If all KC loggers should enable or disable MDC context
   */
  public static void toggleGlobalMdcLoggingContext(boolean shouldPrependMdcContext) {
    prependMdcContext = shouldPrependMdcContext;
    META_LOGGER.debug(
        "Setting MDC context enablement to: {}. MDC context is only available for Apache Kafka"
            + " versions after 2.3.0",
        shouldPrependMdcContext);
  }

  /**
   * Create and return a new logging handler
   *
   * @param name The class name passed for initializing the logger
   */
  public KCLogger(String name) {
    this.logger = LoggerFactory.getLogger(name);
  }

  /**
   * Logs an info level message
   *
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void info(String format, Object... vars) {
    if (this.logger.isInfoEnabled()) {
      this.logger.info(this.getFormattedLogMessage(format, vars));
    }
  }

  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  /**
   * Logs an trace level message
   *
   * @param format The message format without variables
   * @param vars The variables to insert into the format. These variables will be toString()'ed
   */
  public void trace(String format, Object... vars) {
    if (this.logger.isTraceEnabled()) {
      this.logger.trace(this.getFormattedLogMessage(format, vars));
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
      this.logger.debug(this.getFormattedLogMessage(format, vars));
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
      this.logger.warn(this.getFormattedLogMessage(format, vars));
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
      this.logger.error(this.getFormattedLogMessage(format, vars));
    }
  }

  public void error(String s, Throwable throwable) {
    if (this.logger.isErrorEnabled()) {
      logger.error(s, throwable);
    }
  }

  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  private String getFormattedLogMessage(String format, Object... vars) {
    if (prependMdcContext) {
      String connCtx = MDC.get(MDC_CONN_CTX_KEY);
      return Utils.formatLogMessage(connCtx + format, vars);
    }

    return Utils.formatLogMessage(format, vars);
  }
}
