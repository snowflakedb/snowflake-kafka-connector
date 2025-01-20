package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.InternalUtils.convertIngestStatus;
import static com.snowflake.kafka.connector.internal.InternalUtils.timestampToDate;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.security.PrivateKey;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.commons.lang3.StringUtils;

/**
 * Implementation of Snowpipe API calls. i.e handshake between KC and Snowpipe API's.
 *
 * <p>1. ingestFiles
 *
 * <p>2. insertReport - Continuous polling
 *
 * <p>3. loadHistoryScan - for last 1 hour.
 *
 * <p>The difference between above two APIs @see <a
 * href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html">here</a>
 */
public class SnowflakeIngestionServiceV1 implements SnowflakeIngestionService {

  private final KCLogger LOGGER = new KCLogger(SnowflakeIngestionServiceV1.class.getName());
  private static final long ONE_HOUR = 60 * 60 * 1000;

  private final String stageName;
  private final SimpleIngestManager ingestManager;
  private final String pipeName;
  private SnowflakeTelemetryService telemetry = null;

  private String beginMark = null;

  SnowflakeIngestionServiceV1(
      String accountName,
      String userName,
      String host,
      int port,
      String connectionScheme,
      String stageName,
      String pipeName,
      PrivateKey privateKey,
      String userAgentSuffix,
      @Nullable SnowflakeTelemetryService telemetry) {

    this(
        stageName,
        pipeName,
        createOrThrow(
            accountName,
            userName,
            host,
            port,
            connectionScheme,
            pipeName,
            privateKey,
            userAgentSuffix,
            telemetry),
        telemetry);
    LOGGER.info("initialized the pipe connector for pipe {}", pipeName);
  }

  @VisibleForTesting
  SnowflakeIngestionServiceV1(
      String stageName,
      String pipeName,
      SimpleIngestManager ingestManager,
      @Nullable SnowflakeTelemetryService telemetry) {
    this.stageName = stageName;
    this.pipeName = pipeName;
    this.ingestManager = ingestManager;
    this.telemetry = telemetry;
  }

  private static SimpleIngestManager createOrThrow(
      String accountName,
      String userName,
      String host,
      int port,
      String connectionScheme,
      String pipeName,
      PrivateKey privateKey,
      String userAgentSuffix,
      SnowflakeTelemetryService telemetry) {
    try {
      return new SimpleIngestManager(
          accountName,
          userName,
          pipeName,
          privateKey,
          connectionScheme,
          host,
          port,
          userAgentSuffix);
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_0002.getException(e, telemetry);
    }
  }

  @Override
  public void setTelemetry(SnowflakeTelemetryService telemetry) {
    this.telemetry = telemetry;
  }

  @Override
  public void ingestFile(final String fileName) {
    try {
      InternalUtils.backoffAndRetry(
          telemetry,
          SnowflakeInternalOperations.INSERT_FILES_SNOWPIPE_API,
          () -> ingestManager.ingestFile(new StagedFileWrapper(fileName), null));
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_3001.getException(e, this.telemetry);
    }
    LOGGER.debug("ingest file: {}", fileName);
  }

  @Override
  public void ingestFiles(final List<String> fileNames) {
    if (fileNames.isEmpty()) {
      LOGGER.info("ingest files: [Nothing to ingest]");
      return;
    }

    String debugInfo =
        LOGGER.isDebugEnabled()
            ? String.format("\nfileNames: %s", Arrays.toString(fileNames.toArray()))
            : StringUtils.EMPTY;
    LOGGER.info("ingest files: {}{}", fileNames.size(), debugInfo);

    try {
      InternalUtils.backoffAndRetry(
          telemetry,
          SnowflakeInternalOperations.INSERT_FILES_SNOWPIPE_API,
          () -> {
            while (fileNames.size() > 0) {
              // Can not send more than 5000 files in one request,
              // so batch 4000 as one request
              int toIndex = Math.min(4000, fileNames.size());
              List<String> fileNamesBatch = fileNames.subList(0, toIndex);
              Set<String> fileNamesSet = new HashSet<>(fileNamesBatch);
              ingestManager.ingestFiles(SimpleIngestManager.wrapFilepaths(fileNamesSet), null);
              fileNamesBatch.clear();
            }
            return true;
          });
    } catch (Exception e) {
      LOGGER.error("Failed ingest files: {}", Arrays.toString(fileNames.toArray()));
      throw SnowflakeErrors.ERROR_3001.getException(e, this.telemetry);
    }
  }

  @Override
  public String getStageName() {
    return this.stageName;
  }

  @Override
  public Map<String, InternalUtils.IngestedFileStatus> readIngestReport(final List<String> files) {
    Map<String, InternalUtils.IngestedFileStatus> fileStatus = initFileStatus(files);

    if (fileStatus.size() == 0) {
      return fileStatus;
    }

    HistoryResponse response;
    try {
      response =
          (HistoryResponse)
              InternalUtils.backoffAndRetry(
                  telemetry,
                  SnowflakeInternalOperations.INSERT_REPORT_SNOWPIPE_API,
                  () -> ingestManager.getHistory(null, null, beginMark));
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_3002.getException(e, this.telemetry);
    }

    int numOfRecords = 0;

    if (response != null) {
      beginMark = response.getNextBeginMark();

      if (response.files != null) {
        for (HistoryResponse.FileEntry file : response.files) {
          if (fileStatus.containsKey(file.getPath())) {
            numOfRecords++;

            final InternalUtils.IngestedFileStatus ingestionStatus =
                convertIngestStatus(file.getStatus());
            fileStatus.put(file.getPath(), ingestionStatus);
            // Log errors
            if (InternalUtils.IngestedFileStatus.FAILED.equals(ingestionStatus)
                || InternalUtils.IngestedFileStatus.PARTIALLY_LOADED.equals(ingestionStatus)) {
              LOGGER.warn("Failed to load file {} for pipe {}", file.getPath(), this.pipeName);
            }
          }
        }
      }
    }

    LOGGER.info("searched {} files in ingest report, found {}", files.size(), numOfRecords);

    return fileStatus;
  }

  @Override
  public Map<String, InternalUtils.IngestedFileStatus> readOneHourHistory(
      final List<String> files, final long startTime) {
    long endTime = startTime + ONE_HOUR;
    Map<String, InternalUtils.IngestedFileStatus> result = initFileStatus(files);
    Map<String, InternalUtils.IngestedFileStatus> response =
        checkHistoryByRange(startTime, endTime);

    files.forEach(
        name -> {
          if (response.containsKey(name)) {
            result.put(name, response.get(name));
          }
        });

    return result;
  }

  @Override
  public int readIngestHistoryForward(
      Map<String, InternalUtils.IngestedFileStatus> storage,
      Predicate<HistoryResponse.FileEntry> fileFilter,
      AtomicReference<String> historyMarker,
      Integer lastNSeconds) {
    HistoryResponse response;
    try {
      response =
          (HistoryResponse)
              InternalUtils.backoffAndRetry(
                  telemetry,
                  SnowflakeInternalOperations.INSERT_REPORT_SNOWPIPE_API,
                  () -> ingestManager.getHistory(null, lastNSeconds, historyMarker.get()));
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_3002.getException(e, this.telemetry);
    }

    AtomicInteger loadedRecords = new AtomicInteger();
    if (response != null) {
      ArrayList<String> loadedFiles = new ArrayList<>();
      historyMarker.set(response.getNextBeginMark());
      response.files.stream()
          .filter(file -> fileFilter == null || fileFilter.test(file))
          .forEach(
              historyEntry -> {
                storage.compute(
                    historyEntry.getPath(),
                    (key, status) -> convertIngestStatus(historyEntry.getStatus()));
                loadedRecords.incrementAndGet();
                loadedFiles.add(historyEntry.getPath());
              });
      LOGGER.info(
          "loaded {} files out of {} in ingest report since marker {}",
          loadedRecords.get(),
          response.files.size(),
          historyMarker.get());
      if (LOGGER.isDebugEnabled()) {
        List<String> historyFiles =
            response.files.stream()
                .map(HistoryResponse.FileEntry::getPath)
                .collect(Collectors.toList());
        LOGGER.debug(
            "Read from ingest history following files: {}, but loaded: {}",
            String.join(", ", historyFiles),
            String.join(", ", loadedFiles));
      }
    }
    return loadedRecords.get();
  }

  /**
   * check files status from load history
   *
   * @param start start timestamp inclusive
   * @param end end timestamp exclusive
   * @return a map contains file status
   */
  private Map<String, InternalUtils.IngestedFileStatus> checkHistoryByRange(long start, long end) {
    long currentTime = System.currentTimeMillis();
    if (start > currentTime) {
      start = currentTime;
    }

    if (end > currentTime) {
      end = currentTime;
    }
    Map<String, InternalUtils.IngestedFileStatus> result = new HashMap<>();

    HistoryRangeResponse response;

    String startTimeInclusive = timestampToDate(start);

    String endTimeExclusive = timestampToDate(end);

    while (!startTimeInclusive.equals(endTimeExclusive)) {
      try {
        final String startTimeInclusiveFinal = startTimeInclusive;
        response =
            (HistoryRangeResponse)
                InternalUtils.backoffAndRetry(
                    telemetry,
                    SnowflakeInternalOperations.LOAD_HISTORY_SCAN_SNOWPIPE_API,
                    () ->
                        ingestManager.getHistoryRange(
                            null, (String) startTimeInclusiveFinal, endTimeExclusive));
      } catch (Exception e) {
        throw SnowflakeErrors.ERROR_1002.getException(e, this.telemetry);
      }
      if (response != null && response.files != null) {
        response.files.forEach(
            entry -> result.put(entry.getPath(), convertIngestStatus(entry.getStatus())));
      } else {
        throw SnowflakeErrors.ERROR_4001.getException(
            "the response of load history is null", this.telemetry);
      }

      LOGGER.info(
          "read load history between {} and {}. retrieved {} records.",
          startTimeInclusive,
          endTimeExclusive,
          response.files.size());

      startTimeInclusive = response.getEndTimeExclusive();
    }
    return result;
  }

  @Override
  public void close() {
    try {
      ingestManager.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close ingestManager: " + e.getMessage());
    }
    LOGGER.info("IngestService Closed");
  }

  /**
   * create an file status map and set all status to not_processed
   *
   * @param files a list of file names
   * @return a map contains file status
   */
  private Map<String, InternalUtils.IngestedFileStatus> initFileStatus(List<String> files) {
    Map<String, InternalUtils.IngestedFileStatus> result = new HashMap<>();

    for (String fileName : files) {
      result.put(fileName, InternalUtils.IngestedFileStatus.NOT_FOUND);
    }

    return result;
  }

  /* Only used for testing */
  public SimpleIngestManager getIngestManager() {
    return this.ingestManager;
  }
}
