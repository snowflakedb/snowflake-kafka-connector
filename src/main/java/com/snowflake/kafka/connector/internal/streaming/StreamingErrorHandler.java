package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG;

import com.google.common.base.Strings;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/** Class encapsulating logic related to error handling e.g. DLQ. */
public class StreamingErrorHandler {

  private static final KCLogger LOGGER = new KCLogger(StreamingErrorHandler.class.getName());

  private final boolean logErrors;
  private final boolean isDLQTopicSet;
  private final boolean errorTolerance;
  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;
  private final SnowflakeTelemetryService telemetryServiceV2;

  public StreamingErrorHandler(
      Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SnowflakeTelemetryService telemetryServiceV2) {

    this.logErrors = StreamingUtils.logErrors(sfConnectorConfig);
    this.isDLQTopicSet = !Strings.isNullOrEmpty(StreamingUtils.getDlqTopicName(sfConnectorConfig));
    this.errorTolerance = StreamingUtils.tolerateErrors(sfConnectorConfig);
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;
    this.telemetryServiceV2 = telemetryServiceV2;
  }

  public void handleError(Exception error, SinkRecord kafkaSinkRecord) {
    if (logErrors) {
      LOGGER.error("Insert Row Error message:{}", error.getMessage());
    }
    if (errorTolerance) {
      if (!isDLQTopicSet) {
        LOGGER.warn(
            "{} is set, however {} is not. The message will not be added to the Dead Letter Queue"
                + " topic.",
            ERRORS_TOLERANCE_CONFIG,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
      } else {
        LOGGER.warn(
            "Adding the message to Dead Letter Queue topic: {}",
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
        // Wrap in DataException for KCv3 compatibility while preserving original exception
        DataException wrappedException =
            new DataException("Error converting record: " + error.getMessage(), error);
        this.kafkaRecordErrorReporter.reportError(kafkaSinkRecord, wrappedException);
      }
    } else {
      // Preserve the record in DLQ before failing the task
      if (isDLQTopicSet && kafkaRecordErrorReporter != null) {
        LOGGER.warn(
            "Routing failed record to DLQ topic before aborting task (errors.tolerance=none)");
        DataException wrappedException =
            new DataException("Error converting record: " + error.getMessage(), error);
        this.kafkaRecordErrorReporter.reportError(kafkaSinkRecord, wrappedException);
      }
      final String errMsg =
          String.format(
              "Error inserting Records using Streaming API with msg:%s", error.getMessage());
      this.telemetryServiceV2.reportKafkaConnectFatalError(errMsg);
      throw new DataException(errMsg, error);
    }
  }
}
