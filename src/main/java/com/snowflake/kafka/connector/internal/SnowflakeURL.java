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

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Snowflake URL Object https://account.region.snowflakecomputing.com:443 */
public class SnowflakeURL implements URL {

  private final KCLogger LOGGER = new KCLogger(SnowflakeURL.class.getName());

  private String jdbcUrl;

  private final String url;

  private final boolean ssl;

  private final String account;

  private final int port;

  /**
   * There are several matching groups here. Matching groups numbers are identified as the opening
   * braces start and are indexed from number 1.
   *
   * <p>Group 1: If https is present or not. (Not required)
   *
   * <p>Group 2: Is the entire URL including the port number
   *
   * <p>Group 3: URL until .com
   *
   * <p>Group 4: Account name (may include org-account/alias)
   *
   * <p>Group 5: (Everything after accountname or org-accountname until .com)
   *
   * <p>Group 7: port number
   */
  private static final String SNOWFLAKE_URL_REGEX_PATTERN =
      "^(https?://)?((([\\w\\d-]+)(\\.[\\w\\d-]+){2,})(:(\\d+))?)/?$";

  public SnowflakeURL(String urlStr) {
    Pattern pattern = Pattern.compile(SNOWFLAKE_URL_REGEX_PATTERN);

    Matcher matcher = pattern.matcher(urlStr.trim().toLowerCase());

    if (!matcher.find()) {
      throw SnowflakeErrors.ERROR_0007.getException("input url: " + urlStr);
    }

    ssl = !"http://".equals(matcher.group(1));

    url = matcher.group(3);

    account = matcher.group(4);

    if (matcher.group(7) != null) {
      port = Integer.parseInt(matcher.group(7));
    } else if (ssl) {
      port = 443;
    } else {
      port = 80;
    }

    jdbcUrl = "jdbc:snowflake://" + url + ":" + port;
    if (enableJDBCTrace()) {
      LOGGER.info("enabling JDBC tracing");
      jdbcUrl = jdbcUrl + "/?tracing=ALL";
    }

    LOGGER.debug("parsed Snowflake URL: {}", urlStr);
  }

  /**
   * Read environment variable JDBC_TRACE to check whether trace is enabled
   *
   * @return whether to enable JDBC trace
   */
  boolean enableJDBCTrace() {
    String enableJDBCTrace = System.getenv(SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_TRACE);
    return enableJDBCTrace != null && enableJDBCTrace.toLowerCase().contains("true");
  }

  String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getAccount() {
    return account;
  }

  @Override
  public boolean sslEnabled() {
    return ssl;
  }

  public String getScheme() {
    if (ssl) {
      return "https";
    } else {
      return "http";
    }
  }

  String getFullUrl() {
    return url + ":" + port;
  }

  public String getUrlWithoutPort() {
    return url;
  }

  int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return getFullUrl();
  }

  @Override
  public String hostWithPort() {
    return getFullUrl();
  }

  @Override
  public String path() {
    return OAuthConstants.TOKEN_REQUEST_ENDPOINT;
  }
}
