package com.snowflake.kafka.connector.internal;

import java.util.Collection;
import java.util.Map;

/**
 * Mbean Interface for registering JMX metrics. These are the metrics visible as Mbeans when JMX
 * hostname and port are configured.
 *
 * <p>Contains metrics related to Offsets (Kafka Offsets)
 *
 * <p>Contains metrics related to files(Files that were created from offsets and were ingested
 * through Snowpipe)
 */
public interface SnowflakeTelemetryPipeStatusMBean {

  // ------------ Metrics related to Offsets (Kafka Offsets) ------------ //

  /**
   * Offset number that is most recent inside the buffer (In memory buffer)
   *
   * <p>This is updated every time an offset is sent as put API of SinkTask {@link
   * org.apache.kafka.connect.sink.SinkTask#put(Collection)}
   *
   * @return long offset
   */
  long getProcessedOffset();

  /**
   * Offset number(Record) that is being flushed into an internal stage after the buffer threshold
   * was reached. Buffer can reach its threshold by either time, number of records or size.
   *
   * @return long offset
   */
  long getFlushedOffset();

  /**
   * Offset number (Record) for which precommit {@link
   * org.apache.kafka.connect.sink.SinkTask#preCommit(Map)} API was called and we called snowpipe's
   * insertFiles API.
   *
   * @return long offset
   */
  long getCommittedOffset();

  /**
   * Offsets which are being purged from internal stage. (This number is the highest recent most
   * offset which was purged from internal stage)
   *
   * @return long offset
   */
  long getPurgedOffset();

  // ------------ Metrics related to File counts at various stage ------------ //

  /**
   * @return number of files currently on an internal stage. The value will be decremented once
   *     files are being purged. These gives an estimate on how many files are present on an
   *     internal stage at any given point of time.
   */
  long getFileCountOnInternalStage();

  /**
   * @return number of files we call insertFiles API in snowpipe. Note: There is currently a
   *     limitation of 10k files being sent to a single rest request. So these metric has no one to
   *     one relation between files and number of REST API calls. Number of REST API call for
   *     insertFiles can be larger than this value. The value drop backs to zero if there are no
   *     more files to be ingested.
   */
  long getFileCountOnIngestion();

  /**
   * @return number of files present on table stage due to failed ingestion (Missing Ingestion
   *     Status).
   */
  long getFileCountFailedIngestionOnTableStage();

  /**
   * @return number of files present on table stage because files corresponds to broken offset
   *     (Broken record)
   */
  long getFileCountBrokenRecordOnTableStage();
}
