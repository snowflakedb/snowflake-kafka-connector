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
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
    .ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Snowflake Converter
 */
public abstract class SnowflakeConverter implements Converter
{

  protected static Logger LOGGER =
      LoggerFactory.getLogger(SnowflakeConverter.class.getName());

  static ObjectMapper MAPPER = new ObjectMapper();

  /**
   * unused
   */
  @Override
  public void configure(final Map<String, ?> map, final boolean b)
  {
    //not necessary
  }

  /**
   * doesn't support data source connector
   */
  @Override
  public byte[] fromConnectData(final String s, final Schema schema, final
  Object o)
  {
    throw SnowflakeErrors.ERROR_5002.getException();
  }

}
