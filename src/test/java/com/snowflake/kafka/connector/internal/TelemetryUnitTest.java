package com.snowflake.kafka.connector.internal;

import org.junit.Test;

public class TelemetryUnitTest {

  @Test
  public void testEmptyPipeStatusObject()
  {
    String table = "table";
    String stage = "stage";
    String pipe = "pipe";
    SnowflakeTelemetryPipeStatus pipeStatus = new SnowflakeTelemetryPipeStatus(table, stage, pipe);
    assert pipeStatus.empty();
    pipeStatus.averageCommitLagFileCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageCommitLag.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageIngestionLagFileCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageIngestionLag.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageKafkaLagRecordCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.averageKafkaLag.set(1);
    assert !pipeStatus.empty();
    pipeStatus.memoryUsage.set(1);
    assert !pipeStatus.empty();
    pipeStatus.cleanerRestartCount.set(1);
    assert !pipeStatus.empty();
    pipeStatus.fileCountTableStageBrokenRecord.set(1);
    assert !pipeStatus.empty();
    pipeStatus.fileCountTableStageIngestFail.set(1);
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
