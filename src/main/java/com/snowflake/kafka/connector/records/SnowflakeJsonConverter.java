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
import org.apache.kafka.connect.data.SchemaAndValue;


public class SnowflakeJsonConverter extends SnowflakeConverter
{

  /**
   * cast bytes array to Json array
   * @param s topic name. unused
   * @param bytes input bytes array, only support single json record now
   * @return JSON array
   */
  @Override
  public SchemaAndValue toConnectData(final String s, final byte[] bytes)
  {
    try
    {
      //always return an array of JsonNode.
      //AVRO record may contains multiple records
      JsonNode[] result = {MAPPER.readTree(bytes)};
      return new SchemaAndValue(new SnowflakeJsonSchema(), result);
    } catch (Exception ex)
    {
      LOGGER.error("Can't cast input record to JSON object\n{}", ex
          .getMessage());
      throw new IllegalArgumentException("Can't cast input record to JSON " +
          "object\n" + ex.getMessage());
    }
  }
}
