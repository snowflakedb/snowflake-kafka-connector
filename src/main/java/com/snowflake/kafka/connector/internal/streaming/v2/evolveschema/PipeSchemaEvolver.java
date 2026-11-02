package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.data.Schema;

/**
 * Evolves each pipe's target table to the record value schemas a task sees. Calls the evolve-schema
 * endpoint once per distinct schema per pipe; shared by the task's partition channels, so channels
 * writing to the same pipe don't repeat the call.
 */
public class PipeSchemaEvolver {

  private static final KCLogger LOGGER = new KCLogger(PipeSchemaEvolver.class.getName());

  /** Schemas remembered per pipe; the set is cleared when it reaches this size. */
  static final int MAX_SCHEMAS_PER_PIPE = 100;

  private final EvolveSchemaClient client;
  private final String database;
  private final String schema;
  private final Map<String, EvolvedSchemas> evolvedByPipe = new ConcurrentHashMap<>();
  private volatile boolean notEnabled;

  /** What {@link #evolveIfNeeded} did. */
  public enum Result {
    /** No call was needed: the schema was already evolved, or isn't a struct. */
    NOT_NEEDED,
    /** The pipe's table was evolved to the schema. */
    EVOLVED,
    /** Snowflake answered that evolve-schema is off for the account; the evolver is now off. */
    NOT_ENABLED
  }

  public PipeSchemaEvolver(EvolveSchemaClient client, String database, String schema) {
    this.client = client;
    this.database = database;
    this.schema = schema;
  }

  /**
   * Evolves {@code pipe}'s table to {@code columnSchema}, unless it was already evolved for {@code
   * valueSchema}. Does nothing if {@code valueSchema} is not a struct, or once evolve-schema was
   * found to be off for the account.
   *
   * @param pipe pipe the record is appended to
   * @param channelName channel the record is appended to
   * @param offset Kafka offset of the record
   * @param valueSchema the record's value schema, used to recognize schemas already evolved
   * @param columnSchema the schema to send: {@code valueSchema} with top-level field names as the
   *     column names the connector writes
   * @return what was done
   * @throws EvolveSchemaException if the call fails, other than because evolve-schema is off for
   *     the account
   */
  public Result evolveIfNeeded(
      String pipe, String channelName, long offset, Schema valueSchema, Schema columnSchema) {
    if (notEnabled || valueSchema == null || valueSchema.type() != Schema.Type.STRUCT) {
      return Result.NOT_NEEDED;
    }
    EvolvedSchemas evolved = evolvedByPipe.computeIfAbsent(pipe, p -> new EvolvedSchemas());
    synchronized (evolved) {
      // Converters reuse Schema instances, so the identity check skips hashing on most records.
      if (notEnabled || valueSchema == evolved.last) {
        return Result.NOT_NEEDED;
      }
      Result result = Result.NOT_NEEDED;
      if (!evolved.schemas.contains(valueSchema)) {
        EvolveSchemaResponse response;
        try {
          response =
              client.evolveSchema(
                  database,
                  schema,
                  pipe,
                  new EvolveSchemaRequest(
                      EvolveSchemaRequest.SCHEMA_TYPE_KAFKA_CONNECT,
                      ConnectSchemaJson.toJson(columnSchema),
                      channelName,
                      Long.toString(offset),
                      false));
        } catch (EvolveSchemaException e) {
          if (!e.isNotEnabled()) {
            throw e;
          }
          notEnabled = true;
          LOGGER.warn(
              "Evolve-schema is off for the account ({}); columns are added by the backend's"
                  + " schema evolution instead",
              e.getMessage());
          return Result.NOT_ENABLED;
        }
        LOGGER.info(
            "Evolved schema for pipe {} at channel {} offset {}: added={}, typeChanged={},"
                + " madeNullable={}, pipeShutdown={}",
            pipe,
            channelName,
            offset,
            response.getColumnsAdded(),
            response.getColumnsTypeChanged(),
            response.getColumnsMadeNullable(),
            response.isPipeShutdown());
        if (evolved.schemas.size() >= MAX_SCHEMAS_PER_PIPE) {
          evolved.schemas.clear();
        }
        evolved.schemas.add(valueSchema);
        result = Result.EVOLVED;
      }
      evolved.last = valueSchema;
      return result;
    }
  }

  private static final class EvolvedSchemas {
    private final Set<Schema> schemas = new HashSet<>();
    private Schema last;
  }
}
