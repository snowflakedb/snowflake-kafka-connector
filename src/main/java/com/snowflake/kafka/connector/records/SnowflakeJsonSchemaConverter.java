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

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

/**
 * SnowflakeJsonSchemaConverter is a converter for JSON_SR serialization format.
 */
public class SnowflakeJsonSchemaConverter extends SnowflakeJsonConverter {
  private final KafkaJsonSchemaDeserializer<Map<String, Object>> deserializer = new KafkaJsonSchemaDeserializer<>();

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    try {
      super.configure(configs, isKey);
      deserializer.configure(configs, isKey);
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_0012.getException(e);
    }
  }

  /**
   * cast bytes array to Json array
   *
   * @param topic topic name
   * @param bytes input bytes array, only support single json record now
   * @return JSON array
   */
  @Override
  public SchemaAndValue toConnectData(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent());
    }

    return super.toConnectData(topic, deserializeBytesFromJsonSchema(topic, bytes));
  }

  private byte[] deserializeBytesFromJsonSchema(String topic, byte[] bytes) {
    try {
      return mapper.writeValueAsString(deserializer.deserialize(topic, bytes)).getBytes();
    } catch (JsonProcessingException e) {
      LOGGER.error("Failed to deserialize bytes from JsonSchema", e);
      return new byte[0];
    }
  }
}

