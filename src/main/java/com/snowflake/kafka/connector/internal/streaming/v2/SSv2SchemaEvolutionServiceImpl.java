package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionTargetItems;
import com.snowflake.kafka.connector.internal.streaming.validation.RowSchema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Implementation of evolution service that alters table schema, recreates the pipe in a safe way
 */
public class SSv2SchemaEvolutionServiceImpl implements SSv2SchemaEvolutionService {

  private static final KCLogger LOGGER =
      new KCLogger(SSv2SchemaEvolutionServiceImpl.class.getName());

  private final StreamingErrorHandler streamingErrorHandler;
  private final SSv2PipeCreator ssv2PipeCreator;
  private final String tableName;
  private final SchemaEvolutionService schemaEvolutionService;
  private final Runnable waitForAllPartitionsToCommitData;
  private final Consumer<String> closeClientAndReopenChannelsForTable;

  public SSv2SchemaEvolutionServiceImpl(
      StreamingErrorHandler streamingErrorHandler,
      String tableName,
      SchemaEvolutionService schemaEvolutionService,
      Map<String, String> connectorConfig,
      SnowflakeConnectionService conn,
      Runnable waitForAllPartitionsToCommitData,
      Consumer<String> closeClientAndReopenChannelsForTable) {
    String pipeName = PipeNameProvider.pipeName(connectorConfig.get(Utils.NAME), tableName);
    this.streamingErrorHandler = streamingErrorHandler;
    this.ssv2PipeCreator = new SSv2PipeCreator(conn, pipeName, tableName);
    this.tableName = tableName;
    this.schemaEvolutionService = schemaEvolutionService;
    this.waitForAllPartitionsToCommitData = waitForAllPartitionsToCommitData;
    this.closeClientAndReopenChannelsForTable = closeClientAndReopenChannelsForTable;
  }

  @Override
  public void evolveSchemaIfNeeded(
      SinkRecord kafkaSinkRecord, Map<String, Object> transformedRecord, RowSchema rowSchema) {
    Map<String, Object> fieldsToValidate = new HashMap<>(transformedRecord);
    // skip RECORD_METADATA cause SSv1 validations don't accept POJOs
    fieldsToValidate.remove("RECORD_METADATA");

    // validation return only first encountered error type so it might be needed to call it more
    // than once. Possible validation errors are: extra column, null value for non-null column or
    // missing value for non-null column
    int typesOfValidationErrors = 3;
    for (int i = 0; i < typesOfValidationErrors + 1; i++) {
      RowSchema.Error error = rowSchema.validate(fieldsToValidate);
      if (error == null) {
        // no errors - data can be safely ingested to Snowflake
        return;
      } else {
        boolean triggersSchemaEvolution =
            error.extraColNames() != null
                || error.nullValueForNotNullColNames() != null
                || error.missingNotNullColNames() != null;
        if (triggersSchemaEvolution) {
          LOGGER.info(
              "Record doesn't match table schema - triggering schema evolution. topic={},"
                  + " partition={}, offset={}",
              kafkaSinkRecord.topic(),
              kafkaSinkRecord.kafkaOffset(),
              kafkaSinkRecord.kafkaPartition());
          waitForAllPartitionsToCommitData.run();
          evolveSchemaIfNeeded(kafkaSinkRecord, error, rowSchema);
          ssv2PipeCreator.createPipe(CreatePipeMode.CREATE_OR_REPLACE);
          closeClientAndReopenChannelsForTable.accept(tableName);
        } else {
          LOGGER.info(
              "Record doesn't match table schema. This can't be fixed by schema evolution."
                  + " topic={}, partition={}, offset={}",
              kafkaSinkRecord.topic(),
              kafkaSinkRecord.kafkaOffset(),
              kafkaSinkRecord.kafkaPartition());
          streamingErrorHandler.handleError(List.of(error.cause()), kafkaSinkRecord);
          return;
        }
      }
    }
    // should not happen, but it is reasonable to send record to DLQ instead of stopping connector
    LOGGER.warn("Unexpected state in schema evolution. Record will be sent to DLQ if possible.");
    streamingErrorHandler.handleError(
        List.of(new IllegalStateException("Schema evolution unsuccessful")), kafkaSinkRecord);
  }

  private void evolveSchemaIfNeeded(
      SinkRecord kafkaSinkRecord, RowSchema.Error error, RowSchema rowSchema) {
    SchemaEvolutionTargetItems targetItems =
        new SchemaEvolutionTargetItems(
            tableName,
            Utils.joinNullableLists(
                error.missingNotNullColNames(), error.nullValueForNotNullColNames()),
            error.extraColNames());
    schemaEvolutionService.evolveSchemaIfNeeded(
        targetItems, kafkaSinkRecord, rowSchema.getColumnProperties());
  }
}
