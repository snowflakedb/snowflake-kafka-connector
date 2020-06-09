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

import org.junit.Test;

public class SnowflakeURLTest
{

  @Test
  public void createFromValidURL()
  {
    String url = "http://account.snowflake.com:80";

    SnowflakeURL sfurl = new SnowflakeURL(url);

    assert ! sfurl.sslEnabled();

    assert sfurl.getAccount().equals("account");

    assert sfurl.getFullUrl().equals("account.snowflake.com:80");

    assert sfurl.getPort() == 80;

    assert sfurl.getScheme().equals("http");

    assert sfurl.getJdbcUrl().equals("jdbc:snowflake://" + sfurl.getFullUrl());

    url = "https://account.snowflake.com:443";

    sfurl = new SnowflakeURL(url);

    assert sfurl.sslEnabled();

    assert sfurl.getScheme().equals("https");

    assert sfurl.getAccount().equals("account");

    url = " account.snowflake.com:80";

    sfurl = new SnowflakeURL(url);

    assert sfurl.sslEnabled();

    assert sfurl.getAccount().equals("account");

    assert sfurl.getFullUrl().equals("account.snowflake.com:80");

    assert sfurl.getPort() == 80;

    assert sfurl.getScheme().equals("https");

    assert sfurl.getJdbcUrl().equals("jdbc:snowflake://" + sfurl.getFullUrl());

    url = "account.snowflake.com";

    new SnowflakeURL(url);

    url = "http://account.snowflake.com ";

    sfurl = new SnowflakeURL(url);

    assert ! sfurl.sslEnabled();

    assert sfurl.getAccount().equals("account");

    assert sfurl.getFullUrl().equals("account.snowflake.com:80");

    assert sfurl.getPort() == 80;

    assert sfurl.getScheme().equals("http");

    assert sfurl.getJdbcUrl().equals("jdbc:snowflake://" + sfurl.getFullUrl());

    url = "https://account.snowflake.com";

    new SnowflakeURL(url);

    url = "https://account.region.aws.privatelink.snowflake.com:443";

    sfurl = new SnowflakeURL(url);

    assert sfurl.getUrlWithoutPort().equals("account.region.aws.privatelink.snowflake.com");
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void createFromInvalidURL()
  {
    String url = "htt://account.snowflake.com:80";

    new SnowflakeURL(url);
  }

}
