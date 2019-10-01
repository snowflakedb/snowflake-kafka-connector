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

import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * SnowflakeSinkTask implements SinkTask for Kafka Connect framework.
 * expects configuration from SnowflakeSinkConnector
 * creates sink service instance
 * takes records loaded from those Kafka partitions,
 * ingests to Snowflake via Sink service
 */
public class SnowflakeSinkTask extends SinkTask
{
  // connector configuration
  private Map<String, String> config;

  private Map<String, String> topic2table;
  // config buffer.count.records -- how many records to buffer
  private long bufferCountRecords;
  // config buffer.size.bytes -- aggregate size in bytes of all records to buffer
  private long bufferSizeBytes;
  private long bufferFlushTime;

  private SnowflakeSinkService sink;

  // snowflake JDBC connection provides methods to interact with user's snowflake
  // account and execute queries
  private SnowflakeConnectionService conn;

  private static final Logger LOGGER = LoggerFactory
    .getLogger(SnowflakeSinkTask.class);

  /**
   * default constructor, invoked by kafka connect framework
   */
  public SnowflakeSinkTask()
  {
    //nothing
  }

  /**
   * start method handles configuration parsing and one-time setup of the
   * task. loads configuration
   * @param parsedConfig - has the configuration settings
   */
  @Override
  public void start(final Map<String, String> parsedConfig)
  {
    LOGGER.info(Logging.logMessage("SnowflakeSinkTask:start"));

    this.config = parsedConfig;

    //generate topic to table map
    this.topic2table = getTopicToTableMap(config);

    //enable jvm proxy
    Utils.enableJVMProxy(config);

    this.bufferCountRecords = Long.parseLong(config.get
      (SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
    this.bufferSizeBytes = Long.parseLong(config.get
      (SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
    this.bufferFlushTime = Long.parseLong(config.get
      (SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));

    conn = SnowflakeConnectionServiceFactory
      .builder()
      .setProperties(parsedConfig)
      .build();
  }

  /**
   * stop method is invoked only once outstanding calls to other methods
   * have completed.
   * e.g. after current put, and a final preCommit has completed.
   */
  @Override
  public void stop()
  {
    LOGGER.info(Logging.logMessage("SnowflakeSinkTask:stop"));

  }

  /**
   * init ingestion task in Sink service
   *
   * @param partitions - The list of all partitions that are now assigned to
   *                   the task
   */
  @Override
  public void open(final Collection<TopicPartition> partitions)
  {
    LOGGER.info(Logging.logMessage(
      "SnowflakeSinkTask:open, TopicPartitions: {}", partitions
    ));

    SnowflakeSinkServiceFactory.SnowflakeSinkServiceBuilder sinkBuilder =
      SnowflakeSinkServiceFactory.builder(conn)
      .setFileSize(bufferSizeBytes)
      .setRecordNumber(bufferCountRecords)
      .setFlushTime(bufferFlushTime);

    partitions.forEach(
      partition -> {
        String tableName = tableName(partition.topic(), topic2table);
        sinkBuilder.addTask(tableName, partition.topic(), partition.partition());
      }
    );

    sink = sinkBuilder.build();
  }


  /**
   * close sink service
   * close all running task because the parameter of open function contains all
   * partition info but not only the new partition
   * @param partitions - The list of all partitions that were assigned to the
   *                   task
   */
  @Override
  public void close(final Collection<TopicPartition> partitions)
  {
    sink.close();
  }

  /**
   * ingest records to Snowflake
   *
   * @param records - collection of records from kafka topic/partitions for
   *                this connector
   */
  @Override
  public void put(final Collection<SinkRecord> records)
  {
    records.forEach(sink::insert);
  }

  /**
   * Sync committed offsets
   *
   * @param offsets - the current map of offsets as of the last call to put
   * @return a map of offsets by topic-partition that are safe to commit
   * @throws RetriableException when meet any issue during processing
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
    Map<TopicPartition, OffsetAndMetadata> offsets)
    throws RetriableException
  {

    Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

    offsets.forEach(
      (topicPartition, offsetAndMetadata) ->
      {
        long offSet = sink.getOffset(topicPartition);
        if(offSet == 0)
        {
          committedOffsets.put(topicPartition, offsetAndMetadata);
          //todo: update offset?
        }
        else
        {
          committedOffsets.put(topicPartition, new OffsetAndMetadata(sink.getOffset(topicPartition)));
        }
      }
    );

    return committedOffsets;
  }

  /**
   * @return connector version
   */
  @Override
  public String version()
  {
    return Utils.VERSION;
  }

  /**
   * parse topic to table map
   * @param config connector config file
   * @return result map
   */
  static Map<String,String> getTopicToTableMap(Map<String, String> config)
  {
    if(config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP))
    {
      Map<String, String> result =
        Utils.parseTopicToTableMap(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP));
      if(result != null)
      {
        return result;
      }
      LOGGER.error(Logging.logMessage(
        "Invalid Input, Topic2Table Map disabled"
      ));
    }
    return new HashMap<>();

  }

  /**
   * verify topic name, and generate valid table name
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return table name
   */
  static String tableName(String topic, Map<String, String> topic2table)
  {
    final String PLACE_HOLDER = "_";
    if(topic == null || topic.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0020.getException("topic name: " + topic);
    }
    if(topic2table.containsKey(topic))
    {
      return topic2table.get(topic);
    }
    if(Utils.isValidSnowflakeObjectIdentifier(topic))
    {
      return topic;
    }
    int hash = Math.abs(topic.hashCode());

    StringBuilder result = new StringBuilder();

    int index = 0;
    //first char
    if(topic.substring(index,index + 1).matches("[_a-zA-Z]"))
    {
      result.append(topic.charAt(0));
      index ++;
    }
    else
    {
      result.append(PLACE_HOLDER);
    }
    while(index < topic.length())
    {
      if (topic.substring(index, index + 1).matches("[_$a-zA-Z0-9]"))
      {
        result.append(topic.charAt(index));
      }
      else
      {
        result.append(PLACE_HOLDER);
      }
      index ++;
    }

    result.append(PLACE_HOLDER);
    result.append(hash);

    return result.toString();
  }

}
