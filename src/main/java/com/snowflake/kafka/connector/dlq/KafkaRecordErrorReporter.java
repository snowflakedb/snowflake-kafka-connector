package com.snowflake.kafka.connector.dlq;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This interface is a wrapper on top of {@link ErrantRecordReporter}. This allows tolerating
 * situations when the class {@link ErrantRecordReporter} is not available because it was recently
 * added and backported to older versions.
 *
 * @see <a
 *     href="https://javadoc.io/doc/org.apache.kafka/connect-api/2.6.0/org/apache/kafka/connect/sink/ErrantRecordReporter.html">
 *     Documentation </a>
 */
public interface KafkaRecordErrorReporter {
  void reportError(SinkRecord record, Exception e);
}
