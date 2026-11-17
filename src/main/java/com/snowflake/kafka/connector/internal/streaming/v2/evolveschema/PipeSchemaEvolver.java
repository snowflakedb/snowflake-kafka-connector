package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.EvolveSchemaResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SchemaType;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.apache.kafka.connect.data.Schema;

/**
 * Evolves each pipe's target table to the record value schemas a task sees. Calls the SDK's {@code
 * evolveSchema} once per distinct schema per pipe; shared by the task's partition channels, so
 * channels writing to the same pipe don't repeat the call.
 */
public class PipeSchemaEvolver {

  private static final KCLogger LOGGER = new KCLogger(PipeSchemaEvolver.class.getName());

  /** Schemas remembered per pipe; the set is cleared when it reaches this size. */
  static final int MAX_SCHEMAS_PER_PIPE = 100;

  /** How long the evolver stays off after Snowflake answers that evolve-schema is not enabled. */
  static final Duration NOT_ENABLED_RETRY_INTERVAL = Duration.ofMinutes(5);

  /** The SDK's error code name for Snowflake's 403 {@code ERR_EVOLVE_SCHEMA_NOT_ENABLED}. */
  static final String EVOLVE_SCHEMA_NOT_ENABLED = "EvolveSchemaNotEnabled";

  /** The SDK's error code name for a request Snowflake declined (its 400s and 404s). */
  public static final String SF_API_USER_ERROR = "SfApiUserError";

  /** Snowflake's code for a schema it can't parse or map to columns. */
  static final String ERR_EVOLVE_SCHEMA_INVALID_SCHEMA = "ERR_EVOLVE_SCHEMA_INVALID_SCHEMA";

  /**
   * Prefix of Snowflake's codes for a schema past a column limit (too many columns, a name too
   * long, a reserved name).
   */
  static final String ERR_CHANNEL_SCHEMA_EVOLUTION_PREFIX = "ERR_CHANNEL_SCHEMA_EVOLUTION_";

  private final LongSupplier nanoTime;
  private final Map<String, EvolvedSchemas> evolvedByPipe = new ConcurrentHashMap<>();
  // System.nanoTime() before which the evolver is off; 0 when it's on.
  private volatile long notEnabledUntilNanos;

  /** What {@link #evolveIfNeeded} did. */
  public enum Result {
    /**
     * No call was needed: the schema was already evolved, isn't a struct, or the evolver is off.
     */
    NOT_NEEDED,
    /** The pipe's table was evolved to the schema. */
    EVOLVED,
    /**
     * Snowflake answered that evolve-schema is off for the account; the evolver is off for {@link
     * #NOT_ENABLED_RETRY_INTERVAL}.
     */
    NOT_ENABLED
  }

  public PipeSchemaEvolver() {
    this(System::nanoTime);
  }

  @VisibleForTesting
  PipeSchemaEvolver(LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
  }

  /**
   * Whether the connector evolves tables through the SDK: the evolve-schema feature is on, with
   * server-side validation and schematization, the only rows whose columns come from the record
   * schema.
   */
  public static boolean isEnabled(SinkTaskConfig config) {
    return config.isEvolveSchemaEnabled()
        && config.getValidation() == SnowflakeValidation.SERVER_SIDE
        && config.isEnableSchematization();
  }

  /**
   * Evolves {@code pipe}'s table to {@code columnSchema}, unless it was already evolved for {@code
   * valueSchema}. Does nothing if {@code valueSchema} is not a struct, or while the evolver is off
   * because evolve-schema was found to be off for the account.
   *
   * @param client the SDK client for {@code pipe}
   * @param pipe pipe the record is appended to
   * @param channelName channel the record is appended to, for logging
   * @param offset Kafka offset of the record, for logging
   * @param valueSchema the record's value schema, used to recognize schemas already evolved
   * @param columnSchema the schema to send: {@code valueSchema} with top-level field names as the
   *     column names the connector writes
   * @return what was done
   * @throws SFException if the call fails, other than because evolve-schema is off for the account
   */
  public Result evolveIfNeeded(
      SnowflakeStreamingIngestClient client,
      String pipe,
      String channelName,
      long offset,
      Schema valueSchema,
      Schema columnSchema) {
    if (isOff() || valueSchema == null || valueSchema.type() != Schema.Type.STRUCT) {
      return Result.NOT_NEEDED;
    }
    EvolvedSchemas evolved = evolvedByPipe.computeIfAbsent(pipe, p -> new EvolvedSchemas());
    synchronized (evolved) {
      // Converters reuse Schema instances, so the identity check skips hashing on most records.
      if (isOff() || valueSchema == evolved.last) {
        return Result.NOT_NEEDED;
      }
      SFException rejection = evolved.rejected.get(valueSchema);
      if (rejection != null) {
        // Snowflake already rejected this schema itself; asking again would get the same answer.
        throw rejection;
      }
      Result result = Result.NOT_NEEDED;
      if (!evolved.schemas.contains(valueSchema)) {
        EvolveSchemaResult evolveResult;
        try {
          evolveResult =
              client.evolveSchema(
                  SchemaType.KAFKA_CONNECT,
                  ConnectSchemaJson.toJson(columnSchema).toString(),
                  /* normalizeColumnNames= */ false);
        } catch (SFException e) {
          if (isRejectionOfTheSchema(e)) {
            if (evolved.rejected.size() >= MAX_SCHEMAS_PER_PIPE) {
              evolved.rejected.clear();
            }
            evolved.rejected.put(valueSchema, e);
          }
          if (!EVOLVE_SCHEMA_NOT_ENABLED.equals(e.getErrorCodeName())) {
            throw e;
          }
          long until = nanoTime.getAsLong() + NOT_ENABLED_RETRY_INTERVAL.toNanos();
          // 0 means on, so a deadline that lands on 0 moves 1ns later.
          notEnabledUntilNanos = until == 0 ? 1 : until;
          LOGGER.warn(
              "Evolve-schema is off for the account ({}); columns are added by the backend's"
                  + " schema evolution instead. Retrying in {}",
              e.getMessage(),
              NOT_ENABLED_RETRY_INTERVAL);
          return Result.NOT_ENABLED;
        }
        LOGGER.info(
            "Evolved schema for pipe {} at channel {} offset {}: added={}, alreadyPresent={},"
                + " madeNullable={}",
            pipe,
            channelName,
            offset,
            evolveResult.getColumnsAdded(),
            evolveResult.getColumnsAlreadyPresent(),
            evolveResult.getColumnsMadeNullable());
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

  /**
   * Forgets the schemas evolved for {@code pipe}, and those Snowflake rejected, so the next record
   * of each schema evolves the table again. Called when a channel opens or reopens: the table may
   * have been dropped and recreated.
   */
  public void forgetPipe(String pipe) {
    evolvedByPipe.remove(pipe);
  }

  /**
   * Whether {@code e} is Snowflake rejecting the schema itself, so the same schema would be
   * rejected again. Rejections about the table or pipe (schema evolution off on the table, a
   * missing privilege, a pipe not found) aren't: a grant or recreation can fix those.
   */
  private static boolean isRejectionOfTheSchema(SFException e) {
    String code = e.getSnowflakeErrorCode();
    return SF_API_USER_ERROR.equals(e.getErrorCodeName())
        && code != null
        && (code.equals(ERR_EVOLVE_SCHEMA_INVALID_SCHEMA)
            || code.startsWith(ERR_CHANNEL_SCHEMA_EVOLUTION_PREFIX));
  }

  private boolean isOff() {
    long until = notEnabledUntilNanos;
    if (until == 0) {
      return false;
    }
    if (nanoTime.getAsLong() - until < 0) {
      return true;
    }
    notEnabledUntilNanos = 0;
    return false;
  }

  private static final class EvolvedSchemas {
    private final Set<Schema> schemas = new HashSet<>();
    private final Map<Schema, SFException> rejected = new HashMap<>();
    private Schema last;
  }
}
