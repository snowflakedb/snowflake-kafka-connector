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
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ArrayNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
  .ObjectNode;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public class RecordService extends Logging
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String OFFSET = "offset";
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String KEY = "key";
  private static final String CONTENT = "content";
  private static final String META = "meta";
  private static final String SCHEMA_ID = "schema_id";
  private static final String HEADERS = "headers";

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

    ObjectNode meta = MAPPER.createObjectNode();
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
    if (record.keySchema().toString().equals(Schema.STRING_SCHEMA.toString()))
    {
      meta.put(KEY, record.key().toString());
    }

    if (!record.headers().isEmpty())
    {
      meta.set(HEADERS, parseHeaders(record.headers()));
    }


    StringBuilder buffer = new StringBuilder();
    for (JsonNode node : content.getData())
    {
      ObjectNode data = MAPPER.createObjectNode();
      data.set(CONTENT, node);
      data.set(META, meta);
      buffer.append(data.toString());
    }
    return buffer.toString();
  }

  static JsonNode parseHeaders(Headers headers)
  {
    ObjectNode result = MAPPER.createObjectNode();
    for (Header header: headers)
    {
      convertData(header.key(), header.value(), header.schema(), result);
    }
    return result;
  }
  private static void convertData(String key, Object value, Schema schema, JsonNode node)
  {
    switch (schema.type())
    {
      case INT8:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (byte) value);
        }
        else
        {
          ((ArrayNode) node).add((byte) value);
        }
        break;
      case INT16:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (short) value);
        }
        else
        {
          ((ArrayNode) node).add((short) value);
        }
        break;
      case INT32:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (int) value);
        }
        else
        {
          ((ArrayNode) node).add((int) value);
        }
        break;
      case INT64:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (long) value);
        }
        else
        {
          ((ArrayNode) node).add((long) value);
        }
        break;
      case FLOAT32:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (float) value);
        }
        else
        {
          ((ArrayNode) node).add((float) value);
        }
        break;
      case FLOAT64:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (double) value);
        }
        else
        {
          ((ArrayNode) node).add((double) value);
        }
        break;
      case BOOLEAN:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (boolean) value);
        }
        else
        {
          ((ArrayNode) node).add((boolean) value);
        }
        break;
      case STRING:
        if (node instanceof ObjectNode)
        {
          ((ObjectNode) node).put(key, (String) value);
        }
        else
        {
          ((ArrayNode) node).add((String) value);
        }
        break;
      case BYTES:
        if (node instanceof ObjectNode)
        {
          assert value instanceof byte[];
          ((ObjectNode) node).put(key, (byte[]) value);
        }
        else
        {
          assert value instanceof byte[];
          ((ArrayNode) node).add((byte[]) value);
        }
        break;
      case STRUCT:
        ObjectNode struct;
        if (node instanceof ObjectNode)
        {
          struct = ((ObjectNode) node).putObject(key);
        }
        else
        {
          struct = ((ArrayNode) node).addObject();
        }
        Struct structContent = (Struct) value;
        for(Field field: schema.fields())
        {
          convertData(field.name(), structContent.get(field), field.schema(), struct);
        }
        break;
      case MAP:
        ObjectNode map;
        if(node instanceof ObjectNode)
        {
          map = ((ObjectNode) node).putObject(key);
        }
        else
        {
          map = ((ArrayNode) node).addObject();
        }
        Map<String, Object> mapContents = (Map<String, Object>) value;
        for(Map.Entry<String, Object> entry: mapContents.entrySet())
        {
          convertData(entry.getKey(), entry.getValue(), schema.valueSchema(), map);
        }
        break;
      case ARRAY:
        ArrayNode array;
        if(node instanceof ObjectNode)
        {
          array = ((ObjectNode) node).putArray(key);
        }
        else
        {
          array = ((ArrayNode) node).addArray();
        }
        List<Object> arrayContents = (List<Object>) value;
        for (Object content: arrayContents)
        {
          convertData(null, content, schema.valueSchema(), array);
        }
    }
  }
}
