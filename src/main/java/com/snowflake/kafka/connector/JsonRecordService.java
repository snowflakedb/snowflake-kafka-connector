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

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
  .ObjectNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
  .ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * JSON Record Service functions
 */
public class JsonRecordService implements RecordService
{

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String OFFSET = "offset";
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String CONTENT = "content";
  private static final String META = "meta";

  private static final Logger LOGGER =
    LoggerFactory.getLogger(JsonRecordService.class.getClass());

  private final boolean includeOffset;

  private final boolean includePartitionNumber;

  private final boolean includeTopic;

  //todo: include timestamp?
  //todo: Since each table only stores one topic,
  //todo: do we really need include topic name here?

  /**
   * process JSON records
   * <p>
   * output JSON format:
   * {
   * "meta":
   * {
   * "offset": 123,
   * "topic": "topic name",
   * "partition": 123,
   * }
   * "content": "record content"
   * }
   *
   * @param includeOffset          if true, the output will contain offset
   * @param includePartitionNumber if true, the output will contain partition
   *                              number
   * @param includeTopic           if true, the output will contain topic name
   */
  public JsonRecordService(boolean includeOffset, boolean
    includePartitionNumber, boolean includeTopic)
  {

    this.includeOffset = includeOffset;

    this.includePartitionNumber = includePartitionNumber;

    this.includeTopic = includeTopic;

    LOGGER.debug(
      "create an instance of JsonRecordService\ninclude offset: {}\ninclude " +
        "partition number: {}\ninclude topic: {}",
      includeOffset,
      includePartitionNumber,
      includeTopic
    );
  }

  /**
   * create a JsonRecordService instance
   */
  public JsonRecordService()
  {
    this(true, true, true);
  }

  /**
   * process given SinkRecord
   *
   * @param record SinkRecord
   * @return a JSON string, already to output
   * @throws IllegalArgumentException if the given record is broken
   */
  @Override
  public String processRecord(final SinkRecord record) throws
    IllegalArgumentException
  {
    ObjectNode data = mapper.createObjectNode();

    JsonNode content;

    try
    {
      //todo: Is value the record content?
      content = mapper.readTree(record.value().toString());

    } catch (IOException e)
    {
      LOGGER.error("error: {} \n when parsing json record: {}", e.getMessage(),
        record.value().toString());

      throw new IllegalArgumentException("Input record is not a valid json " +
        "data");
    }

    ObjectNode meta = mapper.createObjectNode();

    if (includeOffset)
    {
      meta.put(OFFSET, record.kafkaOffset());
    }

    if (includeTopic)
    {
      meta.put(TOPIC, record.topic());
    }

    if (includePartitionNumber)
    {
      meta.put(PARTITION, record.kafkaPartition());
    }

    data.set(CONTENT, content);

    data.set(META, meta);

    return data.toString();
  }

}
