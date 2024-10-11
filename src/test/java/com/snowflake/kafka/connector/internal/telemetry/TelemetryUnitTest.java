package com.snowflake.kafka.connector.internal.telemetry;

import com.codahale.metrics.MetricRegistry;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import org.junit.Test;

public class TelemetryUnitTest {

  @Test
  public void testEmptyPipeStatusObject() {
    String table = "table";
    String stage = "stage";
    String pipe = "pipe";
    String connectorName = "testConnector";
    MetricsJmxReporter metricsJmxReporter =
        new MetricsJmxReporter(new MetricRegistry(), connectorName);
    SnowflakeTelemetryPipeStatus pipeStatus =
        new SnowflakeTelemetryPipeStatus(
            table, stage, pipe, 0, true /* Set true for test*/, metricsJmxReporter);
    assert pipeStatus.isEmpty();
    pipeStatus.setAverageCommitLagFileCount(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setAverageCommitLagMs(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setAverageIngestionLagFileCount(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setAverageIngestionLagMs(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setAverageKafkaLagRecordCount(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setAverageKafkaLagMs(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.addAndGetMemoryUsage(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setCleanerRestartCount(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.updateBrokenRecordMetrics(1l);
    assert !pipeStatus.isEmpty();
    pipeStatus.updateFailedIngestionMetrics(1l);
    assert !pipeStatus.isEmpty();
    pipeStatus.addAndGetFileCountPurged(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.addAndGetFileCountOnIngestion(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.addAndGetFileCountOnStage(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.addAndGetTotalSizeOfData(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.addAndGetTotalNumberOfRecord(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setPurgedOffsetAtomically(operand -> 1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setCommittedOffset(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setFlushedOffset(1);
    assert !pipeStatus.isEmpty();
    pipeStatus.setProcessedOffset(1);
    assert !pipeStatus.isEmpty();
  }
}
