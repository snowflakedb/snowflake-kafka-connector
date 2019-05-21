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
package com.snowflake.kafka.connector;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.snowflake.kafka.connector.Utils.IngestedFileStatus;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SnowPipe connector, each instance manages one pipe
 */
public class SnowflakeIngestService
{

  private static final long ONE_HOUR = 3600 * 1000;

  private static final Logger LOGGER =
    LoggerFactory.getLogger(SnowflakeIngestService.class.getName());

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
   * @throws InvalidKeySpecException
   * @throws NoSuchAlgorithmException
   */
  SnowflakeIngestService(
    String account,
    String user,
    String pipe,
    String host,
    PrivateKey privateKey,
    String stage
  ) throws InvalidKeySpecException, NoSuchAlgorithmException
  {
    this.ingestManager =
      new SimpleIngestManager(account, user, pipe, privateKey, "https",
        host, 443);
    this.stage = stage;

    LOGGER.info("initialized the pipe connector for pipe: " + pipe);
  }

  /**
   * ingest one file
   *
   * @param fileName file name
   * @throws Exception
   */
  void ingestFile(String fileName) throws Exception
  {
    ingestManager.ingestFile(new StagedFileWrapper(fileName), null);

    LOGGER.debug("ingest file " + fileName);
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
    throws IngestResponseException, IOException, URISyntaxException
  {

    Map<String, IngestedFileStatus> fileStatus = initFileStatus(files);

    HistoryResponse response =
      ingestManager.getHistory(null, null, beginMark);

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

    LOGGER.info("searched " + files.size() + " files in ingest report, found "
      + numOfRecords);

    return fileStatus;
  }

  /**
   * Check ingest result of Ingest Report
   *
   * @param fileNames a list of file names being ingested
   * @param timeout   time out in milliseconds
   * @return true for all file loaded, otherwise false
   * @throws Exception when timeout
   */
  boolean checkIngestReport(List<String> fileNames, long timeout)
    throws Exception
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

        LOGGER.debug("file names in waiting list " + Arrays.toString(names
          .toArray()));

      }

      return true;
    });

    LOGGER.info("checking ingest report...");

    return future.get(timeout, TimeUnit.MILLISECONDS);


  }

  /**
   * check process status of given files in Ingest Report and Load History
   *
   * @param files a list of file name
   * @return A Map of file name and status
   */
  Map<String, IngestedFileStatus> checkLoadHistory(List<String> files) throws
    Exception
  {
    long time = System.currentTimeMillis();

    long historyExpiredTime = time - Utils.MAX_RECOVERY_TIME;

    HashSet<String> names = new HashSet<>(files);

    Map<String, IngestedFileStatus> result = initFileStatus(files);

    Map<String, IngestedFileStatus> response;

    while ((!names.isEmpty()) && (time > historyExpiredTime))
    {
      response = checkHistoryByRange(time - ONE_HOUR, time);

      response.forEach((name, status) ->
      {
        if (names.contains(name))
        {
          result.put(name, status);

          names.remove(name);
        }
      });

      //load result hour by hour, do pagination in checkHistoryByRange method
      time -= ONE_HOUR;
    }

    LOGGER.info("searched " + files.size() + " files in load history, found "
      + (files.size() - names.size()));

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
   * @throws Exception if can't access the ingest service
   */
  private Map<String, IngestedFileStatus> checkHistoryByRange(
    long start, long end) throws Exception
  {
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
        LOGGER.error(e.getMessage());

        throw new Exception("Can't access ingest service: " + e);
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
        LOGGER.error("The response of load history is null");

        throw new Exception("service response is null");
      }

      LOGGER.info("read load history between {} and {}. retrieved {} records.",
        startTimeInclusive, endTimeExclusive, response.files.size());

      startTimeInclusive = response.getEndTimeExclusive();
    }


    return result;
  }

}


