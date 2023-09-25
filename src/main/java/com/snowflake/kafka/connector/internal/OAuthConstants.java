/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
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

/**
 * This class contains constants for OAuth request.
 *
 * @see <a
 *     href="https://github.com/snowflakedb/snowflake/blob/4fdb96cd5849f266cda430c5d49a13c29e866af5/GlobalServices/src/main/java/com/snowflake/resources/ResourceConstants.java">ResourceConstants</a>
 */
public class OAuthConstants {
  public static final String TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";
  public static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";
  public static final String BASIC_AUTH_HEADER_PREFIX = "Basic ";
  public static final String GRANT_TYPE_PARAM = "grant_type";
  public static final String REFRESH_TOKEN = "refresh_token";
  public static final String ACCESS_TOKEN = "access_token";
  public static final String REDIRECT_URI = "redirect_uri";
  public static final String DEFAULT_REDIRECT_URI = "https://localhost.com/oauth";
}
