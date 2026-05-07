package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

/**
 * Strategy for replacing an invalid {@link SnowflakeStreamingIngestClient} with a new one.
 *
 * <p>Implementations are expected to use compare-and-swap semantics: if the client has already been
 * replaced by another caller, the existing replacement should be returned without creating a second
 * one.
 */
@FunctionalInterface
public interface ClientRecreator {

  /**
   * Replaces the given invalid client with a new one.
   *
   * @param invalidClient the client instance that is no longer valid (identity-compared in the
   *     pool)
   * @return the new client, or the already-replaced client if another caller got there first
   */
  SnowflakeStreamingIngestClient recreate(SnowflakeStreamingIngestClient invalidClient);
}
