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

import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_HOST;
import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Utils.HTTPS_PROXY_USER;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_HOST;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_PORT;
import static com.snowflake.kafka.connector.Utils.HTTP_PROXY_USER;
import static com.snowflake.kafka.connector.Utils.HTTP_USE_PROXY;
import static com.snowflake.kafka.connector.Utils.JDK_HTTP_AUTH_TUNNELING;
import static com.snowflake.kafka.connector.Utils.buildOAuthHttpPostRequest;
import static com.snowflake.kafka.connector.Utils.getSnowflakeOAuthToken;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.entity.ContentType;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.HttpClientBuilder;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.client.jdbc.internal.google.gson.JsonObject;
import net.snowflake.client.jdbc.internal.google.gson.JsonParser;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.provider.Arguments;

public class TestUtils {
  // test profile properties
  private static final String USER = "user";
  private static final String DATABASE = "database";
  private static final String SCHEMA = "schema";
  private static final String HOST = "host";
  private static final String ROLE = "role";
  private static final String WAREHOUSE = "warehouse";
  private static final String PRIVATE_KEY = "private_key";
  private static final String ENCRYPTED_PRIVATE_KEY = "encrypted_private_key";
  private static final String PRIVATE_KEY_PASSPHRASE = "private_key_passphrase";
  private static final String AUTHENTICATOR = "authenticator";
  private static final String OAUTH_CLIENT_ID = "oauth_client_id";
  private static final String OAUTH_CLIENT_SECRET = "oauth_client_secret";
  private static final String OAUTH_REFRESH_TOKEN = "oauth_refresh_token";
  private static final String OAUTH_TOKEN_ENDPOINT = "oauth_token_endpoint";
  private static final String PASSWORD = "password";

  // AZ request data key
  private static final String AZ_ACCOUNT_NAME = "ACCOUNT_NAME";
  private static final String AZ_LOGIN_NAME = "LOGIN_NAME";
  private static final String AZ_PASSWORD = "PASSWORD";
  private static final String AZ_CLIENT_ID = "clientId";
  private static final String AZ_REDIRECT_URL = "redirectUrl";
  private static final String AZ_RESPONSE_TYPE = "responseType";
  private static final String AZ_SCOPE = "scope";
  private static final String AZ_MASTER_TOKEN = "masterToken";

  // AZ endpoints
  private static final String AZ_LOGIN_ENDPOINT = "/session/authenticate-request";
  private static final String AZ_REQUEST_ENDPOINT = "/oauth/authorization-request";

  // AZ request value
  private static final String AZ_SCOPE_REFRESH_TOKEN_PREFIX = "refresh_token";
  private static final String AZ_SCOPE_ROLE_PREFIX = "session:role:";
  private static final String AZ_RESPONSE_TYPE_CODE = "code";
  private static final String AZ_GRANT_TYPE = "authorization_code";
  private static final String AZ_CREDENTIAL_TYPE_CODE = "code";

  private static final Random random = new Random();
  public static final String TEST_CONNECTOR_NAME = "TEST_CONNECTOR";
  private static final Pattern BROKEN_RECORD_PATTERN =
      Pattern.compile("^[^/]+/[^/]+/(\\d+)/(\\d+)_(key|value)_(\\d+)\\.gz$");

  // profile path
  public static final String PROFILE_PATH = "profile.json";

  private static final ObjectMapper mapper = new ObjectMapper();

  private static Map<String, String> conf = null;

  private static Map<String, String> confWithOAuth = null;

  private static Map<String, String> confForStreaming = null;

  private static SnowflakeURL url = null;

  private static JsonNode profile = null;

  private static JsonNode profileForStreaming = null;

  public static final String JSON_WITH_SCHEMA =
      ""
          + "{\n"
          + "  \"schema\": {\n"
          + "    \"type\": \"struct\",\n"
          + "    \"fields\": [\n"
          + "      {\n"
          + "        \"type\": \"string\",\n"
          + "        \"doc\": \"doc\", \n"
          + "        \"optional\": false,\n"
          + "        \"field\": \"regionid\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"type\": \"string\",\n"
          + "        \"optional\": false,\n"
          + "        \"field\": \"gender\"\n"
          + "      }\n"
          + "    ],\n"
          + "    \"optional\": false,\n"
          + "    \"name\": \"sf.kc.test\"\n"
          + "  },\n"
          + "  \"payload\": {\n"
          + "    \"regionid\": \"Region_5\",\n"
          + "    \"gender\": \"FEMALE\"\n"
          + "  }\n"
          + "}";
  public static final String JSON_WITHOUT_SCHEMA = "{\"userid\": \"User_1\"}";

  private static JsonNode getProfile(final String profileFilePath) {
    if (profileFilePath.equalsIgnoreCase(PROFILE_PATH)) {
      if (profile == null) {
        try {
          profile = mapper.readTree(new File(profileFilePath));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return profile;
    } else {
      if (profileForStreaming == null) {
        try {
          profileForStreaming = mapper.readTree(new File(profileFilePath));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return profileForStreaming;
    }
  }

  /** load all login info a Map from profile file */
  private static Map<String, String> getPropertiesMapFromProfile(final String profileFileName) {
    Map<String, String> configuration = new HashMap<>();

    JsonNode profileJson = getProfile(profileFileName);
    configuration.put(Utils.SF_USER, profileJson.get(USER).asText());
    configuration.put(Utils.SF_DATABASE, profileJson.get(DATABASE).asText());
    configuration.put(Utils.SF_SCHEMA, profileJson.get(SCHEMA).asText());
    configuration.put(Utils.SF_URL, profileJson.get(HOST).asText());
    configuration.put(Utils.SF_WAREHOUSE, profileJson.get(WAREHOUSE).asText());

    if (profileJson.has(AUTHENTICATOR)) {
      configuration.put(Utils.SF_AUTHENTICATOR, profileJson.get(AUTHENTICATOR).asText());
    }
    if (profileJson.has(PRIVATE_KEY)) {
      configuration.put(Utils.SF_PRIVATE_KEY, profileJson.get(PRIVATE_KEY).asText());
    }
    if (profileJson.has(OAUTH_CLIENT_ID)) {
      configuration.put(Utils.SF_OAUTH_CLIENT_ID, profileJson.get(OAUTH_CLIENT_ID).asText());
    }
    if (profileJson.has(OAUTH_CLIENT_SECRET)) {
      configuration.put(
          Utils.SF_OAUTH_CLIENT_SECRET, profileJson.get(OAUTH_CLIENT_SECRET).asText());
    }
    if (profileJson.has(OAUTH_REFRESH_TOKEN)) {
      configuration.put(
          Utils.SF_OAUTH_REFRESH_TOKEN, profileJson.get(OAUTH_REFRESH_TOKEN).asText());
    }
    if (profileJson.has(OAUTH_TOKEN_ENDPOINT)) {
      configuration.put(
          Utils.SF_OAUTH_TOKEN_ENDPOINT, profileJson.get(OAUTH_TOKEN_ENDPOINT).asText());
    }

    // password only appears in test profile
    if (profileJson.has(PASSWORD)) {
      configuration.put(PASSWORD, profileJson.get(PASSWORD).asText());
    }

    configuration.put(Utils.NAME, TEST_CONNECTOR_NAME);

    // enable test query mark
    configuration.put(Utils.TASK_ID, "");

    return configuration;
  }

  static String getEncryptedPrivateKey() {
    return getProfile(PROFILE_PATH).get(ENCRYPTED_PRIVATE_KEY).asText();
  }

  static String getPrivateKeyPassphrase() {
    return getProfile(PROFILE_PATH).get(PRIVATE_KEY_PASSPHRASE).asText();
  }

  /**
   * read private key string from test profile
   *
   * @return a string value represents private key
   */
  public static String getKeyString() {
    return getConf().get(Utils.SF_PRIVATE_KEY);
  }

  public static PrivateKey getPrivateKey() {
    return InternalUtils.parsePrivateKey(TestUtils.getKeyString());
  }

  /**
   * read conf file
   *
   * @return a map of parameters
   */
  public static Map<String, String> getConfFromFileName(final String profileFileName) {
    if (profileFileName.equalsIgnoreCase(PROFILE_PATH)) {
      if (conf == null) {
        conf = getPropertiesMapFromProfile(profileFileName);
      }
      return new HashMap<>(conf);
    } else {
      if (confForStreaming == null) {
        confForStreaming = getPropertiesMapFromProfile(profileFileName);
      }
      return new HashMap<>(confForStreaming);
    }
  }

  /* Get configuration map from profile path. Used against prod deployment of Snowflake */
  public static Map<String, String> getConf() {
    return getConfFromFileName(PROFILE_PATH);
  }

  /* Get configuration map from profile path. Used against prod deployment of Snowflake */
  public static Map<String, String> getConfForStreaming() {
    Map<String, String> configuration = getConfFromFileName(PROFILE_PATH);

    // On top of existing configurations, add
    configuration.put(Utils.SF_ROLE, getProfile(PROFILE_PATH).get(ROLE).asText());
    configuration.put(Utils.TASK_ID, "0");
    configuration.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");
    configuration.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());

    return configuration;
  }

  /* Get configuration map from profile path. Used against prod deployment of Snowflake */
  public static Map<String, String> getConfForStreamingWithOAuth() {
    Map<String, String> configuration = getConfWithOAuth();
    if (configuration != null) {
      // On top of existing configurations, add
      configuration.put(Utils.SF_ROLE, getProfile(PROFILE_PATH).get(ROLE).asText());
      configuration.put(Utils.TASK_ID, "0");
      configuration.put(
          SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
          IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    }
    configuration.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "10");

    return configuration;
  }

  /** @return JDBC config with encrypted private key */
  static Map<String, String> getConfWithEncryptedKey() {
    if (conf == null) {
      getPropertiesMapFromProfile(PROFILE_PATH);
    }
    Map<String, String> config = new HashMap<>(conf);

    config.remove(Utils.SF_PRIVATE_KEY);
    config.put(Utils.SF_PRIVATE_KEY, getEncryptedPrivateKey());
    config.put(Utils.PRIVATE_KEY_PASSPHRASE, getPrivateKeyPassphrase());

    return config;
  }

  public static Map<String, String> getConfWithOAuth() {
    // Return null if we don't have the corresponding OAuth required configuration in the config
    // file
    Map<String, String> conf = getConf();
    if (!conf.containsKey(Utils.SF_OAUTH_CLIENT_ID)
        || !conf.containsKey(Utils.SF_OAUTH_CLIENT_SECRET)) {
      return null;
    }

    if (confWithOAuth == null) {
      confWithOAuth = new HashMap<>(conf);
      assert (confWithOAuth.containsKey(PASSWORD)
              || confWithOAuth.containsKey(Utils.SF_OAUTH_REFRESH_TOKEN))
          && confWithOAuth.containsKey(Utils.SF_OAUTH_CLIENT_ID)
          && confWithOAuth.containsKey(Utils.SF_OAUTH_CLIENT_SECRET);
      if (!confWithOAuth.containsKey(Utils.SF_OAUTH_REFRESH_TOKEN)) {
        confWithOAuth.put(Utils.SF_OAUTH_REFRESH_TOKEN, getRefreshToken(confWithOAuth));
      }
      confWithOAuth.put(Utils.SF_AUTHENTICATOR, Utils.OAUTH);
      confWithOAuth.remove(Utils.SF_PRIVATE_KEY);
      confWithOAuth.put(Utils.SF_ROLE, getProfile(PROFILE_PATH).get(ROLE).asText());
    }
    return new HashMap<>(confWithOAuth);
  }

  /**
   * execute sql query
   *
   * @param query sql query string
   * @return result set
   */
  static ResultSet executeQuery(String query) {
    try {
      Statement statement = TestSnowflakeConnection.getConnection().createStatement();
      return statement.executeQuery(query);
    }
    // if ANY exceptions occur, an illegal state has been reached
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * execute sql query
   *
   * @param query sql query string
   * @param parameter parameter to be inserted at index 1
   */
  public static void executeQueryWithParameter(String query, String parameter) {
    try {
      executeQueryWithParameter(TestSnowflakeConnection.getConnection(), query, parameter);
    } catch (Exception e) {
      throw new RuntimeException("Error executing query: " + query, e);
    }
  }

  /**
   * execute sql query
   *
   * @param conn jdbc connection
   * @param query sql query string
   * @param parameter parameter to be inserted at index 1
   */
  public static void executeQueryWithParameter(Connection conn, String query, String parameter) {
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, parameter);
      stmt.execute();
      stmt.close();
    } catch (Exception e) {
      throw new RuntimeException("Error executing query: " + query, e);
    }
  }

  /**
   * execute sql query and collect result
   *
   * @param query sql query string
   * @param parameter parameter to be inserted at index 1
   * @param resultCollector function to collect result
   * @return result
   * @param <T> result type
   */
  public static <T> T executeQueryAndCollectResult(
      String query, String parameter, Function<ResultSet, T> resultCollector) {
    try {
      return executeQueryAndCollectResult(
          TestSnowflakeConnection.getConnection(), query, parameter, resultCollector);
    } catch (Exception e) {
      throw new RuntimeException("Error executing query: " + query, e);
    }
  }

  /**
   * execute sql query and collect result
   *
   * @param conn jdbc connection
   * @param query sql query string
   * @param parameter parameter to be inserted at index 1
   * @param resultCollector function to collect result
   * @return result
   * @param <T> result type
   */
  public static <T> T executeQueryAndCollectResult(
      Connection conn, String query, String parameter, Function<ResultSet, T> resultCollector) {
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, parameter);
      stmt.execute();
      ResultSet resultSet = stmt.getResultSet();
      T result = resultCollector.apply(resultSet);
      resultSet.close();
      stmt.close();
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Error executing query: " + query, e);
    }
  }

  /**
   * drop a table
   *
   * @param tableName table name
   */
  public static void dropTable(String tableName) {
    String query = "drop table if exists " + tableName;

    executeQuery(query);
  }

  /** Select * from table */
  public static ResultSet showTable(String tableName) {
    String query = "select * from " + tableName;

    return executeQuery(query);
  }

  /**
   * create a random name for test
   *
   * @param objectName e.g. table, stage, pipe
   * @return kafka_connector_test_objectName_randomNum
   */
  private static String randomName(String objectName) {
    long num = random.nextLong();
    num = num < 0 ? (num + 1) * (-1) : num;
    return "kafka_connector_test_" + objectName + "_" + num;
  }

  /** @return a random table name */
  public static String randomTableName() {
    return randomName("table");
  }

  /** @return a random stage name */
  public static String randomStageName() {
    return randomName("stage");
  }

  /** @return a random pipe name */
  public static String randomPipeName() {
    return randomName("pipe");
  }

  public static String randomTopicName() {
    return randomName("topic");
  }

  /**
   * retrieve one properties
   *
   * @param name property name
   * @return property value
   */
  private static String getPropertyValueFromKey(String name) {
    Map<String, String> properties = getConf();

    return properties.get(name);
  }

  static SnowflakeURL getUrl() {
    if (url == null) {
      url = new SnowflakeURL(getProfile(PROFILE_PATH).get(HOST).asText());
    }
    return url;
  }

  /**
   * Check Snowflake Error Code in test
   *
   * @param error Snowflake error
   * @param func function throwing exception
   * @return true is error code is correct, otherwise, false
   */
  public static boolean assertError(SnowflakeErrors error, Runnable func) {
    try {
      func.run();
    } catch (SnowflakeKafkaConnectorException e) {
      return e.checkErrorCode(error);
    }
    return false;
  }

  /** @return snowflake connection for test */
  public static SnowflakeConnectionService getConnectionService() {
    return SnowflakeConnectionServiceFactory.builder().setProperties(getConf()).build();
  }

  /** @return snowflake connection using OAuth authentication for test */
  public static SnowflakeConnectionService getOAuthConnectionService() {
    return SnowflakeConnectionServiceFactory.builder().setProperties(getConfWithOAuth()).build();
  }

  /** @return snowflake streaming ingest connection for test */
  public static SnowflakeConnectionService getConnectionServiceForStreaming() {
    return SnowflakeConnectionServiceFactory.builder().setProperties(getConfForStreaming()).build();
  }

  /** @return snowflake streaming ingest connection using OAuth authentication for test */
  public static SnowflakeConnectionService getOAuthConnectionServiceForStreaming() {
    return SnowflakeConnectionServiceFactory.builder()
        .setProperties(getConfForStreamingWithOAuth())
        .build();
  }

  /**
   * @param configuration map of properties required to set while getting the connection
   * @return snowflake connection for given config map
   */
  public static SnowflakeConnectionService getConnectionService(Map<String, String> configuration) {
    return SnowflakeConnectionServiceFactory.builder().setProperties(configuration).build();
  }

  /**
   * Reset proxy parameters in JVM which is enabled during starting a sink Task. Call this if your
   * test/code executes the Utils.enableJVMProxy function
   */
  public static void resetProxyParametersInJVM() {
    System.setProperty(HTTP_USE_PROXY, "");
    System.setProperty(HTTP_PROXY_HOST, "");
    System.setProperty(HTTP_PROXY_PORT, "");
    System.setProperty(HTTPS_PROXY_HOST, "");
    System.setProperty(HTTPS_PROXY_PORT, "");

    // No harm in unsetting user password as well
    System.setProperty(JDK_HTTP_AUTH_TUNNELING, "");
    System.setProperty(HTTP_PROXY_USER, "");
    System.setProperty(HTTP_PROXY_PASSWORD, "");
    System.setProperty(HTTPS_PROXY_USER, "");
    System.setProperty(HTTPS_PROXY_PASSWORD, "");
  }

  /**
   * retrieve table size from snowflake
   *
   * @param tableName table name
   * @return size of table
   * @throws SQLException if meet connection issue
   */
  public static int tableSize(String tableName) throws SQLException {
    String query = "show tables like '" + tableName + "'";
    ResultSet result = executeQuery(query);

    if (result.next()) {
      return result.getInt("rows");
    }

    return 0;
  }

  /**
   * verify broken record file name is valid
   *
   * @param name file name
   * @return true is file name is valid, false otherwise
   */
  static boolean verifyBrokenRecordName(String name) {
    return BROKEN_RECORD_PATTERN.matcher(name).find();
  }

  /**
   * read partition number from broken record file
   *
   * @param name file name
   * @return partition number
   */
  static int getPartitionFromBrokenFileName(String name) {
    return Integer.parseInt(readFromBrokenFileName(name, 1));
  }

  /**
   * read offset from broken record file
   *
   * @param name file name
   * @return offset
   */
  static long getOffsetFromBrokenFileName(String name) {
    return Long.parseLong(readFromBrokenFileName(name, 2));
  }

  /**
   * Extract info from broken record file
   *
   * @param name file name
   * @param index group index
   * @return target info
   */
  private static String readFromBrokenFileName(String name, int index) {
    Matcher matcher = BROKEN_RECORD_PATTERN.matcher(name);
    if (!matcher.find()) {
      throw SnowflakeErrors.ERROR_0008.getException(("Input file name: " + name));
    }
    return matcher.group(index);
  }

  /** Interface to define the lambda function to be used by assertWithRetry */
  public interface AssertFunction {
    boolean operate() throws Exception;
  }

  /**
   * Assert with sleep and retry logic
   *
   * @param func the lambda function to be asserted defined by interface AssertFunction
   * @param intervalSec retry time interval in seconds
   * @param maxRetry max retry times
   */
  public static void assertWithRetry(AssertFunction func, int intervalSec, int maxRetry)
      throws Exception {
    int iteration = 1;
    while (!func.operate()) {
      if (iteration > maxRetry) {
        throw new InterruptedException("Max retry exceeded");
      }
      Thread.sleep(intervalSec * 1000);
      iteration += 1;
    }
  }

  public static void assertWithRetry(AssertFunction func) throws Exception {
    assertWithRetry(func, 20, 5);
  }

  /* Generate (noOfRecords - startOffset) for a given topic and partition. */
  public static List<SinkRecord> createJsonStringSinkRecords(
      final long startOffset, final long noOfRecords, final String topicName, final int partitionNo)
      throws Exception {
    ArrayList<SinkRecord> records = new ArrayList<>();
    String json = "{ \"f1\" : \"v1\" } ";
    ObjectMapper objectMapper = new ObjectMapper();
    Schema snowflakeSchema = new SnowflakeJsonSchema();
    SnowflakeRecordContent content = new SnowflakeRecordContent(objectMapper.readTree(json));
    for (long i = startOffset; i < startOffset + noOfRecords; ++i) {
      records.add(
          new SinkRecord(
              topicName,
              partitionNo,
              snowflakeSchema,
              content,
              snowflakeSchema,
              content,
              i,
              System.currentTimeMillis(),
              TimestampType.CREATE_TIME));
    }
    return records;
  }

  /* Generate (noOfRecords - startOffset) blank records for a given topic and partition. */
  public static List<SinkRecord> createBlankJsonSinkRecords(
      final long startOffset,
      final long noOfRecords,
      final String topicName,
      final int partitionNo) {
    return createJsonRecords(
        startOffset,
        noOfRecords,
        topicName,
        partitionNo,
        null,
        Collections.singletonMap("schemas.enable", Boolean.toString(false)));
  }

  /* Generate (noOfRecords - startOffset) for a given topic and partition. */
  public static List<SinkRecord> createNativeJsonSinkRecords(
      final long startOffset,
      final long noOfRecords,
      final String topicName,
      final int partitionNo) {
    return createJsonRecords(
        startOffset,
        noOfRecords,
        topicName,
        partitionNo,
        TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8),
        Collections.singletonMap("schemas.enable", Boolean.toString(true)));
  }

  private static List<SinkRecord> createJsonRecords(
      final long startOffset,
      final long noOfRecords,
      final String topicName,
      final int partitionNo,
      byte[] value,
      Map<String, String> converterConfig) {
    JsonConverter converter = new JsonConverter();
    converter.configure(converterConfig, false);
    SchemaAndValue schemaInputValue = converter.toConnectData("test", value);

    ArrayList<SinkRecord> records = new ArrayList<>();
    for (long i = startOffset; i < startOffset + noOfRecords; ++i) {
      records.add(
          new SinkRecord(
              topicName,
              partitionNo,
              Schema.STRING_SCHEMA,
              "test",
              schemaInputValue.schema(),
              schemaInputValue.value(),
              i));
    }
    return records;
  }

  /* Generate (noOfRecords - startOffset) for a given topic and partition which were essentially avro records */
  public static List<SinkRecord> createBigAvroRecords(
      final long startOffset,
      final long noOfRecords,
      final String topicName,
      final int partitionNo) {
    ArrayList<SinkRecord> records = new ArrayList<>();

    final int outerSegmentLength = 10;
    final int innerSegmentLength = 10;
    List<Schema> outerSchemas = new ArrayList<>(outerSegmentLength);
    for (int outerSegment = 0; outerSegment < outerSegmentLength; outerSegment++) {
      SchemaBuilder outerSegmentSchema = SchemaBuilder.struct().name("segment" + outerSegment);
      for (int innerSegment = 0; innerSegment < innerSegmentLength; innerSegment++) {
        outerSegmentSchema.field(
            "segment_" + outerSegment + "_" + innerSegment, Schema.STRING_SCHEMA);
      }
      outerSchemas.add(outerSegmentSchema.build());
    }

    List<Struct> items = new ArrayList<>(outerSegmentLength);
    for (int outerSegment = 0; outerSegment < outerSegmentLength; outerSegment++) {
      Struct outerItem = new Struct(outerSchemas.get(outerSegment));
      for (int innerSegment = 0; innerSegment < innerSegmentLength; innerSegment++) {
        outerItem.put(
            "segment_" + outerSegment + "_" + innerSegment,
            "segment_" + outerSegment + "_" + innerSegment);
      }
      items.add(outerItem);
    }

    SchemaBuilder schemaBuilderBigAvroSegment = SchemaBuilder.struct().name("biggestAvro");
    outerSchemas.forEach(schema -> schemaBuilderBigAvroSegment.field(schema.name(), schema));

    Struct originalBASegment = new Struct(schemaBuilderBigAvroSegment.build());

    for (int i = 0; i < outerSchemas.size(); i++) {
      originalBASegment.put(outerSchemas.get(i).name(), items.get(i));
    }

    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
    byte[] converted =
        avroConverter.fromConnectData(
            topicName, schemaBuilderBigAvroSegment.schema(), originalBASegment);
    SchemaAndValue avroInputValue = avroConverter.toConnectData(topicName, converted);

    for (long i = startOffset; i < startOffset + noOfRecords; ++i) {
      records.add(
          new SinkRecord(
              topicName,
              partitionNo,
              Schema.STRING_SCHEMA,
              "key" + i,
              avroInputValue.schema(),
              avroInputValue.value(),
              i));
    }
    return records;
  }

  /** @deprecated use SnowflakeSinkConnectorConfigBuilder instead */
  @Deprecated
  public static Map<String, String> getConfig() {
    return SnowflakeSinkConnectorConfigBuilder.snowpipeConfig().build();
  }

  /**
   * retrieve client Sequencer for passed channel name associated with table
   *
   * @param tableName table name
   * @param channelName name of channel
   * @throws SQLException if meet connection issue
   */
  public static long getClientSequencerForChannelAndTable(
      String tableName, final String channelName) throws SQLException {
    String query = "show channels in table " + tableName;
    ResultSet result = executeQuery(query);

    while (result.next()) {
      if (result.getString("name").equalsIgnoreCase(channelName)) {
        return result.getInt("client_sequencer");
      }
    }
    return -1;
  }

  /**
   * retrieve offset_token for passed channel name associated with table
   *
   * @param tableName table name * @param channelName name of channel * @throws SQLException if meet
   *     connection issue
   */
  public static long getOffsetTokenForChannelAndTable(String tableName, final String channelName)
      throws SQLException {
    String query = "show channels in table " + tableName;
    ResultSet result = executeQuery(query);

    while (result.next()) {
      if (result.getString("name").equalsIgnoreCase(channelName)) {
        return result.getInt("offset_token");
      }
    }
    return -1;
  }

  /**
   * Check if the schema of the table matches the provided schema.
   *
   * @param tableName the name of the table
   * @param schemaMap the provided schema
   */
  public static void checkTableSchema(String tableName, Map<String, String> schemaMap)
      throws SQLException {
    // the table should be checked to exist beforehand
    InternalUtils.assertNotEmpty("tableName", tableName);
    String describeTableQuery = "desc table " + tableName;
    ResultSet result = executeQuery(describeTableQuery);
    int numberOfColumnExpected = schemaMap.size();
    int numberOfColumnInTable = 0;
    while (result.next()) {
      String colName = result.getString("name");
      if (!colName.equals(colName.toUpperCase())) {
        colName = "\"" + colName + "\"";
      }
      assert result.getString("type").startsWith(schemaMap.get(colName));
      // see if the type of the column in sf is the same as expected (ignoring scale)
      numberOfColumnInTable++;
    }
    assert numberOfColumnExpected == numberOfColumnInTable;
  }

  /**
   * Check if one row retrieved from the table matches the provided content
   *
   * <p>The assumption is that the rows in the table are the same.
   *
   * @param tableName the name of the table
   * @param contentMap the provided content map from columnName to their value
   */
  public static void checkTableContentOneRow(String tableName, Map<String, Object> contentMap)
      throws SQLException {
    InternalUtils.assertNotEmpty("tableName", tableName);
    String getRowQuery = "select * from " + tableName + " limit 1";
    ResultSet result = executeQuery(getRowQuery);
    result.next();
    assert result.getMetaData().getColumnCount() == contentMap.size();
    for (int i = 0; i < contentMap.size(); ++i) {
      String columnName = result.getMetaData().getColumnName(i + 1);
      Object value = result.getObject(i + 1);
      if (value != null) {
        // For map or array
        if (value instanceof String
            && (((String) value).startsWith("{") || ((String) value).startsWith("["))) {
          // Get rid of the formatting added by snowflake
          value = ((String) value).replace(" ", "").replace("\n", "");
        }
        if ("RECORD_METADATA_PLACE_HOLDER".equals(contentMap.get(columnName))) {
          continue;
        }
        assert value.equals(contentMap.get(columnName))
            : "expected: " + contentMap.get(columnName) + " actual: " + value;
      } else {
        assert contentMap.get(columnName) == null : "value should be null";
      }
    }
  }

  public static Map<String, Object> getTableContentOneRow(String tableName) throws SQLException {
    String getRowQuery = "select * from " + tableName + " limit 1";
    ResultSet result = executeQuery(getRowQuery);
    result.next();

    Map<String, Object> contentMap = new HashMap<>();
    for (int i = 0; i < result.getMetaData().getColumnCount(); i++) {
      contentMap.put(result.getMetaData().getColumnName(i + 1), result.getObject(i + 1));
    }
    return contentMap;
  }

  /**
   * Get refresh token using username, password, clientId and clientSecret
   *
   * @param config config parsed from profile
   * @return refresh token
   */
  public static String getRefreshToken(Map<String, String> config) {
    assert config.containsKey(Utils.SF_USER)
        && config.containsKey(Utils.SF_OAUTH_CLIENT_ID)
        && config.containsKey(Utils.SF_OAUTH_CLIENT_SECRET);
    return getSnowflakeOAuthToken(
        getUrl(),
        config.get(Utils.SF_OAUTH_CLIENT_ID),
        config.get(Utils.SF_OAUTH_CLIENT_SECRET),
        getAZCode(config),
        AZ_GRANT_TYPE,
        AZ_CREDENTIAL_TYPE_CODE,
        OAuthConstants.REFRESH_TOKEN);
  }

  private static String getAZCode(Map<String, String> config) {
    assert config.containsKey(PASSWORD)
        && config.containsKey(Utils.SF_USER)
        && config.containsKey(Utils.SF_OAUTH_CLIENT_ID)
        && config.containsKey(Utils.SF_OAUTH_CLIENT_SECRET);

    CloseableHttpClient client = HttpClientBuilder.create().build();
    SnowflakeURL url = getUrl();
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());

    // Build login request
    JsonObject loginData = new JsonObject();
    loginData.addProperty(AZ_ACCOUNT_NAME, url.getAccount().toUpperCase());
    loginData.addProperty(AZ_LOGIN_NAME, config.get(Utils.SF_USER));
    loginData.addProperty(AZ_PASSWORD, config.get(PASSWORD));
    loginData.addProperty(AZ_CLIENT_ID, config.get(Utils.SF_OAUTH_CLIENT_ID));
    loginData.addProperty(AZ_RESPONSE_TYPE, AZ_RESPONSE_TYPE_CODE);
    String scopeString = AZ_SCOPE_REFRESH_TOKEN_PREFIX;
    if (config.containsKey(Utils.SF_ROLE)) {
      scopeString += " " + AZ_SCOPE_ROLE_PREFIX + config.get(ROLE).toUpperCase();
    }
    loginData.addProperty(AZ_SCOPE, scopeString);
    JsonObject loginPayload = new JsonObject();
    loginPayload.add("data", loginData);
    HttpPost loginRequest =
        buildOAuthHttpPostRequest(
            url, AZ_LOGIN_ENDPOINT, headers, buildStringEntity(loginPayload.toString()));

    // Login
    String masterToken;
    try (CloseableHttpResponse httpResponse = client.execute(loginRequest)) {
      String respBodyString = EntityUtils.toString(httpResponse.getEntity());
      JsonObject respBody = JsonParser.parseString(respBodyString).getAsJsonObject();
      masterToken =
          respBody
              .get("data")
              .getAsJsonObject()
              .get(AZ_MASTER_TOKEN)
              .toString()
              .replaceAll("^\"|\"$", "");
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_1004.getException(e);
    }

    // Build AZ code request
    loginData.addProperty(AZ_MASTER_TOKEN, masterToken);
    HttpPost aZCodeRequest =
        buildOAuthHttpPostRequest(
            url, AZ_REQUEST_ENDPOINT, headers, buildStringEntity(loginData.toString()));

    // Request AZ code
    try (CloseableHttpResponse httpResponse = client.execute(aZCodeRequest)) {
      String respBodyString = EntityUtils.toString(httpResponse.getEntity());
      JsonObject respBody = JsonParser.parseString(respBodyString).getAsJsonObject();
      return respBody
          .get("data")
          .getAsJsonObject()
          .get(AZ_REDIRECT_URL)
          .toString()
          .replaceAll(".*\\bcode=([A-Fa-f0-9]+).*", "$1");
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_1004.getException(e);
    }
  }

  private static StringEntity buildStringEntity(String payload) {
    try {
      return new StringEntity(payload);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Stream<Arguments> nBooleanProduct(int n) {
    return Sets.cartesianProduct(
            IntStream.range(0, n)
                .mapToObj(i -> ImmutableSet.of(false, true))
                .collect(Collectors.toList()))
        .stream()
        .map(List::toArray)
        .map(Arguments::of);
  }
}
