package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.InternalUtils.IngestedFileStatus;
import java.util.List;
import java.util.Map;

/** Ingestion service manages snow pipe */
public interface SnowflakeIngestionService {
  /**
   * Set telemetry client
   *
   * @param telemetry telemetry client
   */
  void setTelemetry(SnowflakeTelemetryService telemetry);
  /**
   * Ingest single file
   *
   * @param fileName file name
   */
  void ingestFile(String fileName);

  /**
   * Ingest a list of files
   *
   * @param fileNames file name List
   */
  void ingestFiles(List<String> fileNames);

  /** @return corresponding stage name */
  String getStageName();

  /**
   * retrieve status of given files from the ingest report
   *
   * @param files a list of file name
   * @return a map contains all file status
   */
  Map<String, IngestedFileStatus> readIngestReport(List<String> files);

  /**
   * retrieve status of given files from load history in one hour time window
   *
   * @param files a list of file name
   * @param startTime the start time stamp of time window in ms
   * @return a map contains all file status
   */
  Map<String, IngestedFileStatus> readOneHourHistory(List<String> files, long startTime);

  /**
   * configure the Snowpipe client and return the client sequencer
   *
   * @return a Long value contains the client sequencer
   */
  Long configureClient();

  /**
   * get the Snowpipe client and return the offset token
   *
   * @return a String value contains the offset token
   */
  String getClientStatus();

  /**
   * Ingest a list of files with the clientInfo (clientSequencer and offsetToken)
   *
   * @param fileNames file name List
   * @param clientSequencer unique identification of the Snowpipe client
   */
  void ingestFilesWithClientInfo(List<String> fileNames, long clientSequencer);

  /** close ingest service */
  void close();
}
