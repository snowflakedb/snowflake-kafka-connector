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

import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
  .ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
  .ObjectNode;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class RecordService extends Logging
{
  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String OFFSET = "offset";
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String KEY = "key";
  private static final String CONTENT = "content";
  private static final String META = "meta";
  private static final String SCHEMA_ID = "schema_id";

  /**
   * process records
   * output JSON format:
   * {
   * "meta":
   * {
   * "offset": 123,
   * "topic": "topic name",
   * "partition": 123,
   * "key":"key name"
   * }
   * "content": "record content"
   * }
   * <p>
   * create a JsonRecordService instance
   */
  public RecordService()
  {
  }


  /**
   * process given SinkRecord,
   * only support snowflake converters
   *
   * @param record SinkRecord
   * @return a record string, already to output
   */
  public String processRecord(SinkRecord record)
  {
    if (!record.valueSchema().name().equals(SnowflakeJsonSchema.NAME))
    {
      throw SnowflakeErrors.ERROR_0009.getException();
    }
    if (!(record.value() instanceof SnowflakeRecordContent))
    {
      throw SnowflakeErrors.ERROR_0010
        .getException("Input record should be SnowflakeRecordContent object");
    }

    SnowflakeRecordContent content = (SnowflakeRecordContent) record.value();

    ObjectNode meta = mapper.createObjectNode();
    meta.put(OFFSET, record.kafkaOffset());
    meta.put(TOPIC, record.topic());
    meta.put(PARTITION, record.kafkaPartition());

    //ignore if no timestamp
    if (record.timestampType() != TimestampType.NO_TIMESTAMP_TYPE)
    {
      meta.put(record.timestampType().name, record.timestamp());
    }

    //include schema id if using avro with schema registry
    if (content.getSchemaID() != -1)
    {
      meta.put(SCHEMA_ID, content.getSchemaID());
    }

    //include String key
    if (record.keySchema().equals(Schema.STRING_SCHEMA))
    {
      meta.put(KEY, record.key().toString());
    }

    StringBuilder buffer = new StringBuilder();
    for (JsonNode node : content.getData())
    {
      ObjectNode data = mapper.createObjectNode();
      data.set(CONTENT, node);
      data.set(META, meta);
      buffer.append(data.toString());
    }
    return buffer.toString();
  }
}
