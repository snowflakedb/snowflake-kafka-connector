package com.snowflake.kafka.connector.dlq;

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * In memory implementation of KafkaRecordErrorReporter which mimics sending records to DLQ. Here we
 * simply insert records into an ArrayList
 *
 * <p>Used for testing.
 */
public final class InMemoryKafkaRecordErrorReporter implements KafkaRecordErrorReporter {
  private final List<ReportedRecord> reportedRecords = new ArrayList<>();

  @Override
  public void reportError(final SinkRecord record, final Exception e) {
    reportedRecords.add(new ReportedRecord(record, e));
  }

  public List<ReportedRecord> getReportedRecords() {
    return unmodifiableList(reportedRecords);
  }

  public static final class ReportedRecord {
    private final SinkRecord record;
    private final Throwable e;

    private ReportedRecord(final SinkRecord record, final Throwable e) {
      this.record = record;
      this.e = e;
    }

    public SinkRecord getRecord() {
      return record;
    }

    public Throwable getException() {
      return e;
    }

    @Override
    public String toString() {
      return "ReportedData{" + "record=" + record + ", e=" + e + '}';
    }
  }
}
