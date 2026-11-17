package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.snowflake.ingest.streaming.EvolveSchemaResult;
import com.snowflake.ingest.streaming.SchemaType;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

  private final Map<String, EvolvedSchemas> evolvedByPipe = new ConcurrentHashMap<>();

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
   * valueSchema}. Does nothing if {@code valueSchema} is not a struct.
   *
   * @param client the SDK client for {@code pipe}
   * @param pipe pipe the record is appended to
   * @param channelName channel the record is appended to, for logging
   * @param offset Kafka offset of the record, for logging
   * @param valueSchema the record's value schema, used to recognize schemas already evolved
   * @param columnSchema the schema to send: {@code valueSchema} with top-level field names as the
   *     column names the connector writes
   * @throws com.snowflake.ingest.streaming.SFException if the call fails
   */
  public void evolveIfNeeded(
      SnowflakeStreamingIngestClient client,
      String pipe,
      String channelName,
      long offset,
      Schema valueSchema,
      Schema columnSchema) {
    if (valueSchema == null || valueSchema.type() != Schema.Type.STRUCT) {
      return;
    }
    EvolvedSchemas evolved = evolvedByPipe.computeIfAbsent(pipe, p -> new EvolvedSchemas());
    synchronized (evolved) {
      // Converters reuse Schema instances, so the identity check skips hashing on most records.
      if (valueSchema == evolved.last) {
        return;
      }
      if (!evolved.schemas.contains(valueSchema)) {
        EvolveSchemaResult result =
            client.evolveSchema(
                SchemaType.KAFKA_CONNECT,
                ConnectSchemaJson.toJson(columnSchema).toString(),
                /* normalizeColumnNames= */ false);
        LOGGER.info(
            "Evolved schema for pipe {} at channel {} offset {}: added={}, alreadyPresent={},"
                + " madeNullable={}",
            pipe,
            channelName,
            offset,
            result.getColumnsAdded(),
            result.getColumnsAlreadyPresent(),
            result.getColumnsMadeNullable());
        if (evolved.schemas.size() >= MAX_SCHEMAS_PER_PIPE) {
          evolved.schemas.clear();
        }
        evolved.schemas.add(valueSchema);
      }
      evolved.last = valueSchema;
    }
  }

  /**
   * Forgets the schemas evolved for {@code pipe}, so the next record of each schema evolves the
   * table again. Called when a channel reopens: the table may have been dropped and recreated.
   */
  public void forgetPipe(String pipe) {
    evolvedByPipe.remove(pipe);
  }

  private static final class EvolvedSchemas {
    private final Set<Schema> schemas = new HashSet<>();
    private Schema last;
  }
}
