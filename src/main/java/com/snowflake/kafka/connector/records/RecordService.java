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
package com.snowflake.kafka.connector.records;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
    .ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
    .ObjectNode;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RecordService
{
  protected static final ObjectMapper mapper = new ObjectMapper();

  protected static final String OFFSET = "offset";
  protected static final String TOPIC = "topic";
  protected static final String PARTITION = "partition";
  protected static final String CONTENT = "content";
  protected static final String META = "meta";

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(RecordService.class.getClass());

  protected final boolean includeOffset;
  protected final boolean includePartitionNumber;
  protected final boolean includeTopic;

  /**
   * process records
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
   *                               number
   * @param includeTopic           if true, the output will contain topic name
   */
  public RecordService(boolean includeOffset, boolean
      includePartitionNumber, boolean includeTopic)
  {

    this.includeOffset = includeOffset;

    this.includePartitionNumber = includePartitionNumber;

    this.includeTopic = includeTopic;

    LOGGER.debug(
        "create an instance of JsonRecordService\ninclude offset: {}\ninclude" +
            " partition number: {}\ninclude topic: {}",
        includeOffset,
        includePartitionNumber,
        includeTopic
    );
  }

  /**
   * create a JsonRecordService instance
   */
  public RecordService()
  {
    this(true, true, true);
  }


  /**
   * process given SinkRecord,
   * only support snowflake converters
   *
   * @param record SinkRecord
   * @return a record string, already to output
   * @throws IllegalArgumentException if the given record is broken
   */
  public String processRecord(SinkRecord record) throws IllegalArgumentException
  {

    if(! record.valueSchema().name().equals(SnowflakeJsonSchema.NAME))
    {
      throw new IllegalArgumentException("Only support Snowflake Converters");
    }

    if(! (record.value() instanceof JsonNode[]))
    {
      throw new IllegalArgumentException("invalid input record");
    }

    JsonNode[] contents = (JsonNode[]) record.value();

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

    StringBuilder buffer = new StringBuilder();

    for (JsonNode node: contents)
    {
      ObjectNode data = mapper.createObjectNode();

      data.set(CONTENT, node);

      data.set(META, meta);

      buffer.append(data.toString());
    }

    return buffer.toString();
  }

  /**
   * generate json string for output
   *
   * @param jsonString input json string
   * @param offset     kafka offset
   * @param topic      kafka topic
   * @param partition  kafka partition
   * @return output json string
   * @throws IOException if input string is invalid
   */
  protected String addMetaData(String jsonString, long offset, String topic,
                               int partition) throws IOException
  {
    ObjectNode data = mapper.createObjectNode();

    JsonNode content;

    content = mapper.readTree(jsonString);

    ObjectNode meta = mapper.createObjectNode();

    if (includeOffset)
    {
      meta.put(OFFSET, offset);
    }

    if (includeTopic)
    {
      meta.put(TOPIC, topic);
    }

    if (includePartitionNumber)
    {
      meta.put(PARTITION, partition);
    }

    data.set(CONTENT, content);

    data.set(META, meta);

    return data.toString();

  }
}
