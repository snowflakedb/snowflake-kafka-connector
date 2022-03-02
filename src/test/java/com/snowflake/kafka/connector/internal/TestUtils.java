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

import static com.snowflake.kafka.connector.Utils.*;

import com.snowflake.client.jdbc.SnowflakeDriver;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.io.File;
import java.io.IOException;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

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
  private static final Random random = new Random();
  private static final String DES_RSA_KEY = "des_rsa_key";
  public static final String TEST_CONNECTOR_NAME = "TEST_CONNECTOR";
  private static final Pattern BROKEN_RECORD_PATTERN =
      Pattern.compile("^[^/]+/[^/]+/(\\d+)/(\\d+)_(key|value)_(\\d+)\\.gz$");

  // profile path
  private static final String PROFILE_PATH = "profile.json";

  private static final String PROFILE_PATH_STREAMING_INGEST = "profile_streaming_qa1.json";

  private static final ObjectMapper mapper = new ObjectMapper();

  private static Connection conn = null;

  private static Connection connForStreamingIngestTests = null;

  private static Map<String, String> conf = null;

  private static Map<String, String> confForStreaming = null;

  private static SnowflakeURL url = null;

  private static JsonNode profile = null;

  private static JsonNode profileForStreaming = null;

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

    configuration.put(Utils.SF_USER, getProfile(profileFileName).get(USER).asText());
    configuration.put(Utils.SF_DATABASE, getProfile(profileFileName).get(DATABASE).asText());
    configuration.put(Utils.SF_SCHEMA, getProfile(profileFileName).get(SCHEMA).asText());
    configuration.put(Utils.SF_URL, getProfile(profileFileName).get(HOST).asText());
    configuration.put(Utils.SF_WAREHOUSE, getProfile(profileFileName).get(WAREHOUSE).asText());
    configuration.put(Utils.SF_PRIVATE_KEY, getProfile(profileFileName).get(PRIVATE_KEY).asText());

    configuration.put(Utils.NAME, TEST_CONNECTOR_NAME);

    // enable test query mark
    configuration.put(Utils.TASK_ID, "");

    if (profileFileName.equalsIgnoreCase(PROFILE_PATH_STREAMING_INGEST)) {
      configuration.put(Utils.SF_ROLE, getProfile(profileFileName).get(ROLE).asText());
      configuration.put(Utils.TASK_ID, "0");
    }

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
   * Create snowflake jdbc connection
   *
   * @return jdbc connection
   * @throws Exception when meeting error
   */
  private static Connection getConnection() throws Exception {
    if (conn != null) {
      return conn;
    }

    return generateConnectionToSnowflake(PROFILE_PATH);
  }

  /**
   * Create snowflake jdbc connection for streaming ingest.
   *
   * <p>Please note, snowflake streaming ingest is not yet available in prod accounts, hence we will
   * have test against a test deployment.
   *
   * @return jdbc connection
   * @throws Exception when error is met
   */
  private static Connection getConnectionForStreamingIngest() throws Exception {
    if (connForStreamingIngestTests != null) {
      return connForStreamingIngestTests;
    }

    return generateConnectionToSnowflake(PROFILE_PATH_STREAMING_INGEST);
  }

  /** Given a profile file path name, generate a connection by constructing a snowflake driver. */
  private static Connection generateConnectionToSnowflake(final String profileFileName)
      throws Exception {
    SnowflakeURL url = new SnowflakeURL(getConfFromFileName(profileFileName).get(Utils.SF_URL));

    Properties properties =
        InternalUtils.createProperties(getConfFromFileName(profileFileName), url.sslEnabled());

    Connection connToSnowflake = new SnowflakeDriver().connect(url.getJdbcUrl(), properties);

    return connToSnowflake;
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
    return getConfFromFileName(PROFILE_PATH_STREAMING_INGEST);
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

  /**
   * execute sql query
   *
   * @param query sql query string
   * @return result set
   */
  static ResultSet executeQuery(String query) {
    try {
      Statement statement = getConnection().createStatement();
      return statement.executeQuery(query);
    }
    // if ANY exceptions occur, an illegal state has been reached
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static ResultSet executeQueryForStreaming(String query) {
    try {
      Statement statement = getConnectionForStreamingIngest().createStatement();
      return statement.executeQuery(query);
    }
    // if ANY exceptions occur, an illegal state has been reached
    catch (Exception e) {
      throw new IllegalStateException(e);
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

  /**
   * drop a table
   *
   * @param tableName table name
   */
  public static void dropTableStreaming(String tableName) {
    String query = "drop table if exists " + tableName;

    executeQueryForStreaming(query);
  }

  /** Select * from table */
  static ResultSet showTable(String tableName) {
    String query = "select * from " + tableName;

    return executeQuery(query);
  }

  public static ResultSet showTableForStreaming(String tableName) {
    String query = "select * from " + tableName;

    return executeQueryForStreaming(query);
  }

  static String getDesRsaKey() {
    return getProfile(PROFILE_PATH).get(DES_RSA_KEY).asText();
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
      url = new SnowflakeURL(getPropertyValueFromKey(Utils.SF_URL));
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

  /* Get connection service instance which connects to test instance of snowflake which has streaming ingest enabled */
  public static SnowflakeConnectionService getConnectionServiceForStreamingIngest() {
    return SnowflakeConnectionServiceFactory.builder()
        .setProperties(getConfFromFileName(PROFILE_PATH_STREAMING_INGEST))
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

  /* Get size of table (QA1 deployment) */
  public static int getTableSizeStreaming(String tableName) throws SQLException {
    String query = "show tables like '" + tableName + "'";
    ResultSet result = executeQueryForStreaming(query);

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

  public static Map<String, String> getConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(Utils.NAME, "test");
    config.put(SnowflakeSinkConnectorConfig.TOPICS, "topic1,topic2");
    config.put(SF_URL, "https://testaccount.snowflake.com:443");
    config.put(SF_USER, "userName");
    config.put(Utils.SF_PRIVATE_KEY, "fdsfsdfsdfdsfdsrqwrwewrwrew42314424");
    config.put(SF_SCHEMA, "testSchema");
    config.put(SF_DATABASE, "testDatabase");
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT + "");
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT + "");
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT + "");
    return config;
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
    ResultSet result = executeQueryForStreaming(query);

    if (result.next()) {
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
    ResultSet result = executeQueryForStreaming(query);

    if (result.next()) {
      if (result.getString("name").equalsIgnoreCase(channelName)) {
        return result.getInt("client_sequencer");
      }
    }
    return -1;
  }
}
