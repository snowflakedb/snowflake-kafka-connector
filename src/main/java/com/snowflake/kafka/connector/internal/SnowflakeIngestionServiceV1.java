package com.snowflake.kafka.connector.internal;

import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;

import java.security.PrivateKey;
import java.util.*;

import static com.snowflake.kafka.connector.internal.InternalUtils.convertIngestStatus;
import static com.snowflake.kafka.connector.internal.InternalUtils.timestampToDate;

public class SnowflakeIngestionServiceV1 extends Logging
  implements SnowflakeIngestionService
{
  private static final long ONE_HOUR = 60 * 60 * 1000;

  private final String stageName;
  private final SimpleIngestManager ingestManager;
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
    PrivateKey privateKey
  )
  {
    this.stageName = stageName;
    try
    {
      this.ingestManager = new SimpleIngestManager(accountName, userName,
        pipeName, privateKey, connectionScheme, host, port);
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_0002.getException(e);
    }
    logInfo("initialized the pipe connector for pipe {}", pipeName);
  }

  @Override
  public void setTelemetry(SnowflakeTelemetryService telemetry)
  {
    this.telemetry = telemetry;
  }

  @Override
  public void ingestFile(final String fileName)
  {
    try
    {
      InternalUtils.backoffAndRetry(telemetry,
          () -> ingestManager.ingestFile(new StagedFileWrapper(fileName), null)
      );
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_3001.getException(e);
    }
    logDebug("ingest file: {}", fileName);
  }

  @Override
  public void ingestFiles(final List<String> fileNames)
  {
    if (fileNames.isEmpty())
    {
      return;
    }
    logDebug("ingest files: {}", fileNames);
    try
    {
      InternalUtils.backoffAndRetry(telemetry,
          () ->
          {
            while (fileNames.size() > 0)
            {
              // Can not send more than 5000 files in one request,
              // so batch 4000 as one request
              int toIndex = Math.min(4000, fileNames.size());
              List<String> fileNamesBatch = fileNames.subList(0, toIndex);
              Set<String> fileNamesSet = new HashSet<>(fileNamesBatch);
              ingestManager.ingestFiles(SimpleIngestManager.wrapFilepaths(fileNamesSet), null);
              fileNamesBatch.clear();
            }
            return true;
          }
      );
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_3001.getException(e);
    }
  }

  @Override
  public String getStageName()
  {
    return this.stageName;
  }

  @Override
  public Map<String, InternalUtils.IngestedFileStatus> readIngestReport(final List<String> files)
  {
    Map<String, InternalUtils.IngestedFileStatus> fileStatus =
      initFileStatus(files);

    if (fileStatus.size() == 0)
    {
      return fileStatus;
    }

    HistoryResponse response;
    try
    {
      response = (HistoryResponse) InternalUtils.backoffAndRetry(telemetry,
          () -> ingestManager.getHistory(null, null, beginMark)
      );
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_3002.getException(e);
    }

    int numOfRecords = 0;

    if (response != null)
    {
      beginMark = response.getNextBeginMark();

      if (response.files != null)
      {
        for (HistoryResponse.FileEntry file : response.files)
        {
          if (fileStatus.containsKey(file.getPath()))
          {
            numOfRecords++;

            fileStatus.put(file.getPath(),
              convertIngestStatus(file.getStatus()));

          }
        }
      }
    }

    logInfo("searched {} files in ingest report, found {}", files.size(),
      numOfRecords);

    return fileStatus;
  }

  @Override
  public Map<String, InternalUtils.IngestedFileStatus> readOneHourHistory(
    final List<String> files, final long startTime)
  {
    long endTime = startTime + ONE_HOUR;
    Map<String, InternalUtils.IngestedFileStatus> result =
      initFileStatus(files);
    Map<String, InternalUtils.IngestedFileStatus> response =
      checkHistoryByRange(startTime, endTime);

    files.forEach(
      name ->
      {
        if (response.containsKey(name))
        {
          result.put(name, response.get(name));
        }
      }
    );

    return result;
  }

  /**
   * check files status from load history
   *
   * @param start start timestamp inclusive
   * @param end   end timestamp exclusive
   * @return a map contains file status
   */
  private Map<String, InternalUtils.IngestedFileStatus> checkHistoryByRange(
    long start, long end)
  {
    long currentTime = System.currentTimeMillis();
    if (start > currentTime)
    {
      start = currentTime;
    }

    if (end > currentTime)
    {
      end = currentTime;
    }
    Map<String, InternalUtils.IngestedFileStatus> result = new HashMap<>();

    HistoryRangeResponse response;

    String startTimeInclusive = timestampToDate(start);

    String endTimeExclusive = timestampToDate(end);

    while (!startTimeInclusive.equals(endTimeExclusive))
    {
      try
      {
        final String startTimeInclusiveFinal = startTimeInclusive;
        response = (HistoryRangeResponse) InternalUtils.backoffAndRetry(telemetry,
            () ->
                ingestManager.getHistoryRange(null, (String) startTimeInclusiveFinal, endTimeExclusive)
        );
      } catch (Exception e)
      {
        throw SnowflakeErrors.ERROR_1002.getException(e);
      }
      if (response != null && response.files != null)
      {
        response.files.forEach(entry ->
          result.put(entry.getPath(), convertIngestStatus(entry.getStatus()))
        );
      }
      else
      {
        throw SnowflakeErrors.ERROR_4001.getException("the response of load " +
          "history is null");
      }

      logInfo("read load history between {} and {}. retrieved {} records.",
        startTimeInclusive, endTimeExclusive,
        response.files.size());

      startTimeInclusive = response.getEndTimeExclusive();
    }
    return result;
  }


  @Override
  public void close()
  {
    try
    {
      ingestManager.close();
    }
    catch (Exception e)
    {
      logError("Failed to close ingestManager: " + e.getMessage());
    }
    logInfo("IngestService Closed");
  }

  /**
   * create an file status map and set all status to not_processed
   *
   * @param files a list of file names
   * @return a map contains file status
   */
  private Map<String, InternalUtils.IngestedFileStatus> initFileStatus(List<String> files)
  {
    Map<String, InternalUtils.IngestedFileStatus> result = new HashMap<>();

    for (String fileName : files)
    {
      result.put(fileName, InternalUtils.IngestedFileStatus.NOT_FOUND);
    }

    return result;
  }
}
