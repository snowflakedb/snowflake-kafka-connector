package com.snowflake.kafka.connector;

import java.util.Collection;
import javax.annotation.Nullable;

public interface TopicToTableResolver {
  /**
   * Resolve a Kafka topic name to a Snowflake table name based on the configured mapping. Returns
   * null if no mapping matches, in which case the caller should fall back to default name
   * derivation.
   */
  @Nullable
  String resolve(String topic);

  /**
   * Returns table names known from the mapping configuration. Used for best-effort pre-flight
   * checks at startup.
   */
  Collection<String> tableNames();
}
