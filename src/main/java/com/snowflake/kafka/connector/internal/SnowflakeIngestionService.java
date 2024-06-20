package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.InternalUtils.IngestedFileStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import net.snowflake.ingest.connection.HistoryResponse;

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
   * Retrieve a collection of all ingested files with their status from load history. Fetching can
   * be further customized by:
   * <li>history marker - when content is null, entire available history is loaded and upon method
   *     return, marker will be updated to the last available marker. Subsequent calls will fetch
   *     only history entries created after the marker.
   * <li>limit the history depth by querying only the last N seconds since now. NOTE: this is not
   *     absolute timestamp, so adjust for potential latency by adding some buffer
   * <li>filter the returned file history by supplying a fileFilter predicate. NOTE: this filtering
   *     takes place at client side, so first - try limiting the result set size by providing
   *     history marker / history depth in seconds Important thing - supplied storage container is
   *     mutated by this call by:
   * <li>adding new file entry if none exist
   * <li>updating the state of the file, if file entry exists The method wil never delete any entry
   *     from the map.
   *
   * @param storage - reference to the map where to store status updates for the tracked files
   * @param fileFilter - filter predicate - if provided will be used to filter only files matching
   *     the criteria.
   * @param historyMarker - reference to the history marker - provide null as reference's value for
   *     the first call, method will update the reference with most recent marker, so the next call
   *     will resume with that specific marker
   * @param lastNSeconds - optionally narrow down history to just last N seconds. null for max
   *     history range
   * @return number of loaded history entries
   */
  int readIngestHistoryForward(
      Map<String, IngestedFileStatus> storage,
      Predicate<HistoryResponse.FileEntry> fileFilter,
      AtomicReference<String> historyMarker,
      Integer lastNSeconds);

  /** close ingest service */
  void close();
}
