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

import java.nio.ByteBuffer;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import org.apache.kafka.connect.data.SchemaAndValue;

public class SnowflakeJsonSchemaConverter extends SnowflakeConverter {
  /**
   * cast bytes array to Json array
   *
   * @param s topic name. unused
   * @param bytes input bytes array, only support single json record now
   * @return JSON array
   */
  @Override
  public SchemaAndValue toConnectData(final String s, final byte[] bytes) {
    if (bytes == null) {
      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent());
    }
    ByteBuffer buffer;
    int id;
    try {
      buffer = ByteBuffer.wrap(bytes);
      if (buffer.get() != 0) {
        throw SnowflakeErrors.ERROR_0010.getException("unknown bytes");
      }
      id = buffer.getInt();
    } catch (Exception ex) {
      LOGGER.error("Failed to parse schema-prepended JSON record\n" + ex.getMessage());
      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent(bytes));
    }

    try {
      int length = buffer.limit() - 1 - 4;
      byte[] data = new byte[length];
      buffer.get(data, 0, length);

      return new SchemaAndValue(
              new SnowflakeJsonSchema(), new SnowflakeRecordContent(mapper.readTree(data), id));
    } catch (Exception ex) {
      LOGGER.error("Failed to parse JSON record\n" + ex.toString());
      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent(bytes));
    }
  }
}
