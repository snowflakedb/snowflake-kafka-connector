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

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_USER;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_USER;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_USE_PROXY;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.Utils.JDK_HTTP_AUTH_TUNNELING;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;

public class TestUtils {
  // test profile properties
  private static final KCLogger log = new KCLogger(TestUtils.class.getName());
  private static final String USER = "user";
  private static final String DATABASE = "database";
  private static final String SCHEMA = "schema";
  private static final String HOST = "host";
  private static final String ROLE = "role";
  private static final String WAREHOUSE = "warehouse";
  private static final String PRIVATE_KEY = "private_key";
  private static final String ENCRYPTED_PRIVATE_KEY = "encrypted_private_key";
  private static final String PRIVATE_KEY_PASSPHRASE = "private_key_passphrase";
  private static final String PASSWORD = "password";

  private static final Random random = new Random();
  public static final String TEST_CONNECTOR_NAME = "TEST_CONNECTOR";

  // profile path
  public static final String PROFILE_PATH = "profile.json";

  private static final ObjectMapper mapper = new ObjectMapper();

  private static SnowflakeURL url = null;

  private static JsonNode profile = null;

  private static JsonNode profileForStreaming = null;

  public static final String JSON_WITH_SCHEMA =
      "{\n"
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

  public static PrivateKey generatePrivateKey() {
    KeyPairGenerator keyPairGenerator = null;
    try {
      keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    keyPairGenerator.initialize(2048);
    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    return keyPair.getPrivate();
  }

  public static Map<String, String> transformProfileFileToConnectorConfiguration(
      boolean takeEncryptedKeyAndPassword) {
    Map<String, String> configuration = new HashMap<>();

    JsonNode profileJson = getProfile(PROFILE_PATH);
    configuration.put(
        KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME, profileJson.get(USER).asText());
    configuration.put(
        KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, profileJson.get(ROLE).asText());
    configuration.put(
        KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME, profileJson.get(DATABASE).asText());
    configuration.put(
        KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME, profileJson.get(SCHEMA).asText());
    configuration.put(
        KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, profileJson.get(HOST).asText());
    configuration.put(SnowflakeDataSourceFactory.SF_WAREHOUSE, profileJson.get(WAREHOUSE).asText());

    if (takeEncryptedKeyAndPassword) {
      configuration.put(
          KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
          profileJson.get(ENCRYPTED_PRIVATE_KEY).asText());
      configuration.put(
          SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, profileJson.get(PRIVATE_KEY_PASSPHRASE).asText());
    } else {
      configuration.put(
          KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, profileJson.get(PRIVATE_KEY).asText());
    }

    // password only appears in test profile
    if (profileJson.has(PASSWORD)) {
      configuration.put(PASSWORD, profileJson.get(PASSWORD).asText());
    }

    configuration.put(KafkaConnectorConfigParams.NAME, TEST_CONNECTOR_NAME);

    // enable test query mark
    configuration.put(Utils.TASK_ID, "");

    return configuration;
  }

  public static Map<String, String> getConnectorConfigurationForStreaming(
      boolean takeEncryptedKey) {
    Map<String, String> configuration =
        transformProfileFileToConnectorConfiguration(takeEncryptedKey);
    // On top of existing properties, add
    configuration.put(Utils.TASK_ID, "0");
    configuration.put(SNOWFLAKE_STREAMING_MAX_CLIENT_LAG, "1");

    return configuration;
  }

  /** @return JDBC config with encrypted private key */
  public static String generateAESKey(PrivateKey key, char[] passwd)
      throws IOException, OperatorCreationException {
    Security.addProvider(new BouncyCastleFipsProvider());
    StringWriter writer = new StringWriter();
    JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    PKCS8EncryptedPrivateKeyInfoBuilder pkcs8EncryptedPrivateKeyInfoBuilder =
        new JcaPKCS8EncryptedPrivateKeyInfoBuilder(key);
    pemWriter.writeObject(
        pkcs8EncryptedPrivateKeyInfoBuilder.build(
            new JcePKCSPBEOutputEncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC)
                .setProvider("BCFIPS")
                .build(passwd)));
    pemWriter.close();
    return writer.toString();
  }
  /**
   * execute sql query
   *
   * @param query sql query string
   * @return result set
   */
  static ResultSet executeQuery(String query) {
    try {
      Statement statement =
          NonEncryptedKeyTestSnowflakeConnection.getConnection().createStatement();
      log.debug("Executing query: {}", query);
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
      executeQueryWithParameter(
          NonEncryptedKeyTestSnowflakeConnection.getConnection(), query, parameter);
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
          NonEncryptedKeyTestSnowflakeConnection.getConnection(),
          query,
          parameter,
          resultCollector);
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

  public static void dropPipe(String pipeName) {
    executeQuery("drop pipe if exists " + pipeName);
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

  public static String randomTopicName() {
    return randomName("topic");
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
    return SnowflakeConnectionServiceFactory.builder()
        .setProperties(transformProfileFileToConnectorConfiguration(false))
        .build();
  }

  public static SnowflakeConnectionService getConnectionServiceWithEncryptedKey() {
    return SnowflakeConnectionServiceFactory.builder()
        .setProperties(getConnectorConfigurationForStreaming(true))
        .build();
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
      final int rows = result.getInt("rows");
      log.debug("{} table size is: {}", tableName, rows);
      return rows;
    }

    return 0;
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
      Thread.sleep(intervalSec * 1000L);
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
    return SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
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

  public static int getNumberOfRows(String tableName) throws SQLException {
    String getRowQuery = "select count(*) from " + tableName;
    ResultSet result = executeQuery(getRowQuery);
    result.next();
    return result.getInt(1);
  }

  public static int getNumberOfColumns(String tableName) throws SQLException {
    String getRowQuery = "select * from " + tableName + " limit 1";
    ResultSet result = executeQuery(getRowQuery);
    return result.getMetaData().getColumnCount();
  }

  public static void assertTableRowCount(String tableName, int expectedRowCount)
      throws SQLException {
    int actualRowCount = getNumberOfRows(tableName);
    if (actualRowCount != expectedRowCount) {
      throw new AssertionError(
          String.format(
              "Expected table %s to have %d rows, but it has %d rows",
              tableName, expectedRowCount, actualRowCount));
    }
  }

  public static void assertTableColumnCount(String tableName, int expectedColumnCount)
      throws SQLException {
    int actualColumnCount = getNumberOfColumns(tableName);
    if (actualColumnCount != expectedColumnCount) {
      throw new AssertionError(
          String.format(
              "Expected table %s to have %d columns, but it has %d columns",
              tableName, expectedColumnCount, actualColumnCount));
    }
  }

  public static void assertTableHasColumn(String tableName, String columnName) throws SQLException {
    String getRowQuery = "select * from " + tableName + " limit 1";
    ResultSet result = executeQuery(getRowQuery);
    ResultSetMetaData metaData = result.getMetaData();
    boolean found = false;
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (metaData.getColumnName(i).equalsIgnoreCase(columnName)) {
        found = true;
        break;
      }
    }
    if (!found) {
      throw new AssertionError(
          String.format(
              "Expected table %s to have column %s, but it was not found", tableName, columnName));
    }
  }

  public static List<Map<String, Object>> getTableRows(String tableName) throws SQLException {
    InternalUtils.assertNotEmpty("tableName", tableName);
    String getRowQuery = "select * from " + tableName;
    ResultSet result = executeQuery(getRowQuery);
    ResultSetMetaData metaData = result.getMetaData();
    int columnCount = metaData.getColumnCount();

    List<Map<String, Object>> rows = new ArrayList<>();
    while (result.next()) {
      Map<String, Object> row = new HashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        Object value = result.getObject(i);
        row.put(columnName, value);
      }
      rows.add(row);
    }
    return rows;
  }
}
