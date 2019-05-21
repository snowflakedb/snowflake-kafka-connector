/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.snowflake.kafka.connector.internal;

import java.security.PrivateKey;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.Utils.IngestedFileStatus;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;

/**
 * SnowPipe connector, each instance manages one pipe
 */
public class SnowflakeIngestService extends Logging
{

  public static final long ONE_HOUR = 3600 * 1000;
  public static final long TEN_MINUTES = 10 * 60 * 1000;

  private SimpleIngestManager ingestManager;
  private String stage;

  private String beginMark = null;

  /**
   * create a new ingest manager
   *
   * @param account    snowflake account name
   * @param user       snowflake user name
   * @param pipe       snowflake pipe name
   * @param host       snowflake url
   * @param privateKey private key
   */
  SnowflakeIngestService(
    String account,
    String user,
    String pipe,
    String host,
    PrivateKey privateKey,
    String stage
  )
  {
    try
    {
      this.ingestManager =
        new SimpleIngestManager(account, user, pipe, privateKey, "https",
          host, 443);
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_0002.getException(e);
    }
    this.stage = stage;

    logInfo("initialized the pipe connector for pipe {}", pipe);
  }

  /**
   * ingest one file
   *
   * @param fileName file name
   */
  void ingestFile(String fileName)
  {
    try
    {
      ingestManager.ingestFile(new StagedFileWrapper(fileName), null);
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_3001.getException(e);
    }

    logDebug("ingest file: {}", fileName);
  }


  /**
   * close connection
   */
  void close()
  {
    // do something
  }

  /**
   * @return working stage name of this pipe
   */
  String getStageName()
  {
    return stage;
  }


  /**
   * check process status of given files in Ingest Report
   *
   * @param files a list of file name
   * @return A Map of file name and status
   */
  Map<String, IngestedFileStatus> checkIngestReport(List<String> files)
  {

    Map<String, IngestedFileStatus> fileStatus = initFileStatus(files);

    HistoryResponse response;
    try
    {
      response = ingestManager.getHistory(null, null, beginMark);
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
              Utils.convertIngestStatus(file.getStatus()));

          }
        }
      }
    }

    logInfo("searched {} files in ingest report, found {}", files.size(),
      numOfRecords);

    return fileStatus;
  }

  /**
   * Check ingest result of Ingest Report
   *
   * @param fileNames a list of file names being ingested
   * @param timeout   time out in milliseconds
   * @return true for all file loaded, otherwise false
   * @throws SnowflakeKafkaConnectorException when timeout
   */
  boolean checkIngestReport(List<String> fileNames, long timeout)
    throws SnowflakeKafkaConnectorException
  {
    ExecutorService executor = Executors.newSingleThreadExecutor();

    Future<Boolean> future = executor.submit(() ->
    {
      Map<String, IngestedFileStatus> result;

      List<String> names = fileNames;

      while (!names.isEmpty())
      {
        Thread.sleep(10000);

        result = checkIngestReport(names);

        if (result.containsValue(IngestedFileStatus.FAILED))
        {
          return false;
        }

        names = result.entrySet().stream()
          .filter(entry -> entry.getValue() != IngestedFileStatus.LOADED)
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());

        logDebug("file names is waiting list: {}", Arrays.toString(names
          .toArray()));

      }

      return true;
    });

    logInfo("checking ingest report...");

    try
    {
      return future.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_3003.getException(e);
    }


  }


  /**
   * check process status of given files in one hour Load History
   * @param files a list of file names
   * @param startTime start timestamp
   * @return
   */
  Map<String, IngestedFileStatus> checkOneHourHistory(List<String> files,
                                                      long startTime)
  {
    long endTime = startTime + ONE_HOUR;
    Map<String, IngestedFileStatus> result = initFileStatus(files);
    Map<String, IngestedFileStatus> response = checkHistoryByRange(startTime, endTime);

    files.forEach(
      name ->
      {
        if(response.containsKey(name))
        {
          result.put(name, response.get(name));
        }
      }
    );

    return result;
  }

  /**
   * create an file status map and set all status to not_processed
   *
   * @param files a list of file names
   * @return a map contains file status
   */
  private Map<String, IngestedFileStatus> initFileStatus(List<String> files)
  {
    Map<String, IngestedFileStatus> result = new HashMap<>();

    for (String fileName : files)
    {
      result.put(fileName, IngestedFileStatus.NOT_FOUND);
    }

    return result;
  }

  /**
   * check files status from load history
   *
   * @param start start timestamp inclusive
   * @param end   end timestamp exclusive
   * @return a map contains file status
   */
  private Map<String, IngestedFileStatus> checkHistoryByRange(long start,
                                                              long end)
  {
    long currentTime = System.currentTimeMillis();
    if(start > currentTime)
    {
      start = currentTime;
    }

    if(end > currentTime)
    {
      end = currentTime;
    }
    Map<String, IngestedFileStatus> result = new HashMap<>();

    HistoryRangeResponse response;

    String startTimeInclusive = Utils.timestampToDate(start);

    String endTimeExclusive = Utils.timestampToDate(end);


    while (!startTimeInclusive.equals(endTimeExclusive))
    {
      try
      {
        response = ingestManager.getHistoryRange(null,
          startTimeInclusive, endTimeExclusive);
      } catch (Exception e)
      {
        throw SnowflakeErrors.ERROR_1002.getException(e);
      }
      if (response != null && response.files != null)
      {
        response.files.forEach(entry ->
          result.put(entry.getPath(), Utils.convertIngestStatus(entry
            .getStatus()))
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

}


