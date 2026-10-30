package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

/** Evolves a pipe's target table to a given schema through Snowpipe Streaming v2. */
public interface EvolveSchemaClient {

  /**
   * Adds, widens, or makes nullable the target table's columns so the table accepts rows of {@code
   * request}'s schema, then restarts the pipe. Safe to repeat: columns already in place are
   * reported in {@link EvolveSchemaResponse#getColumnsAlreadyPresent()}.
   *
   * @param database database of the pipe
   * @param schema schema of the pipe
   * @param pipe pipe name, in the same form passed to the streaming SDK
   * @throws EvolveSchemaException if the call fails
   */
  EvolveSchemaResponse evolveSchema(
      String database, String schema, String pipe, EvolveSchemaRequest request);
}
