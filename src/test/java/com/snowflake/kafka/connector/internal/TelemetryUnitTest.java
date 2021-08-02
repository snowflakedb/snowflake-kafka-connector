package com.snowflake.kafka.connector.internal;

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
            table, stage, pipe, true /* Set true for test*/, metricsJmxReporter);
    assert pipeStatus.empty();
    pipeStatus.averageCommitLagFileCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageCommitLagMs.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageIngestionLagFileCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageIngestionLagMs.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageKafkaLagRecordCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageKafkaLagMs.set(1);
    assert !pipeStatus.empty();
    pipeStatus.memoryUsage.set(1);
    assert !pipeStatus.empty();
    pipeStatus.cleanerRestartCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.updateBrokenRecordMetrics(1l);
    assert !pipeStatus.empty();
    pipeStatus.updateFailedIngestionMetrics(1l);
    assert !pipeStatus.empty();
    pipeStatus.fileCountPurged.set(1);
    assert !pipeStatus.empty();
    pipeStatus.fileCountOnIngestion.set(1);
    assert !pipeStatus.empty();
    pipeStatus.fileCountOnStage.set(1);
    assert !pipeStatus.empty();
    pipeStatus.totalSizeOfData.set(1);
    assert !pipeStatus.empty();
    pipeStatus.totalNumberOfRecord.set(1);
    assert !pipeStatus.empty();
    pipeStatus.purgedOffset.set(1);
    assert !pipeStatus.empty();
    pipeStatus.committedOffset.set(1);
    assert !pipeStatus.empty();
    pipeStatus.flushedOffset.set(1);
    assert !pipeStatus.empty();
    pipeStatus.processedOffset.set(1);
    assert !pipeStatus.empty();
  }
}
