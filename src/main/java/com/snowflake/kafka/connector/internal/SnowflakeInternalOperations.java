package com.snowflake.kafka.connector.internal;

/**
 * Enum placeholders for internal operations. Useful mostly in logging and helps figure out where
 * retries are happening.
 */
public enum SnowflakeInternalOperations {

  /* Either because of a purge file since they were ingested in SF table or because they were moved to table stage */
  REMOVE_FILE_FROM_INTERNAL_STAGE,

  FETCH_OAUTH_TOKEN
}
