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
package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.TestUtils;
import com.snowflake.kafka.connector.Utils;
import org.junit.Test;

import java.util.Map;

public class JDBCTest
{
  @Test
  public void testCreateProperties() throws Exception
  {
    Map<String, String> conf = TestUtils.getConf();

    SnowflakeJDBCWrapper.createProperties(conf);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void missingPrivateKey() throws Exception
  {
    Map<String, String> conf = TestUtils.getConf();

    conf.remove(Utils.SF_PRIVATE_KEY);

    SnowflakeJDBCWrapper.createProperties(conf);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void missingSchema() throws Exception
  {
    Map<String, String> conf = TestUtils.getConf();

    conf.remove(Utils.SF_SCHEMA);

    SnowflakeJDBCWrapper.createProperties(conf);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void missingDataBase() throws Exception
  {
    Map<String, String> conf = TestUtils.getConf();

    conf.remove(Utils.SF_DATABASE);

    SnowflakeJDBCWrapper.createProperties(conf);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void missingUser() throws Exception
  {
    Map<String, String> conf = TestUtils.getConf();

    conf.remove(Utils.SF_USER);

    SnowflakeJDBCWrapper.createProperties(conf);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void missingURL() throws Exception
  {
    Map<String, String> conf = TestUtils.getConf();

    conf.remove(Utils.SF_URL);

    SnowflakeJDBCWrapper.createProperties(conf);
  }

}
