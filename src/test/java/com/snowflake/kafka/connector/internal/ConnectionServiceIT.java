package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceV1.USER_AGENT_SUFFIX_FORMAT;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.streaming.ChannelMigrationResponseCode.OFFSET_MIGRATION_SOURCE_CHANNEL_DOES_NOT_EXIST;
import static com.snowflake.kafka.connector.internal.streaming.ChannelMigrationResponseCode.isChannelMigrationResponseSuccessful;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.streaming.ChannelMigrateOffsetTokenResponseDTO;
import com.snowflake.kafka.connector.internal.streaming.ChannelMigrationResponseCode;
import com.snowflake.kafka.connector.internal.streaming.DirectTopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake.SnowflakeSchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryServiceV1;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import net.snowflake.client.jdbc.internal.apache.http.Header;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectionServiceIT {
  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();

  private final String tableName = TestUtils.randomTableName();
  private final String stageName = TestUtils.randomStageName();
  private final String pipeName = TestUtils.randomPipeName();
  private final String tableName1 = TestUtils.randomTableName();
  private final String stageName1 = TestUtils.randomStageName();
  private final String topicName = TestUtils.randomTopicName();

  @Test
  public void testEncryptedKey() {
    // no exception
    SnowflakeConnectionServiceFactory.builder()
        .setProperties(TestUtils.getConfWithEncryptedKey())
        .build();
  }

  @Test
  public void testOAuthAZ() {
    Map<String, String> confWithOAuth = TestUtils.getConfWithOAuth();
    assert confWithOAuth.containsKey(Utils.SF_OAUTH_CLIENT_ID);
    SnowflakeConnectionServiceFactory.builder().setProperties(TestUtils.getConfWithOAuth()).build();
  }

  @Test
  public void testSetSSLProperties() {
    Map<String, String> testConfig = TestUtils.getConf();
    testConfig.put(Utils.SF_URL, "https://sfctest0.snowflakecomputing.com");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("on");
    testConfig.put(Utils.SF_URL, "sfctest0.snowflakecomputing.com");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("on");
    testConfig.put(Utils.SF_URL, "http://sfctest0.snowflakecomputing.com:400");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("off");
  }

  @Test
  public void testUserAgentSuffixInIngestionService() {
    Map<String, String> testConfig = TestUtils.getConf();
    String kafkaProvider = SnowflakeSinkConnectorConfig.KafkaProvider.SELF_HOSTED.name();
    String userAgentExpectedSuffixInHttpHeader =
        String.format(USER_AGENT_SUFFIX_FORMAT, Utils.VERSION, kafkaProvider);
    testConfig.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, kafkaProvider);
    SnowflakeConnectionService conn =
        SnowflakeConnectionServiceFactory.builder().setProperties(testConfig).build();
    SnowflakeIngestionServiceV1 ingestionService =
        (SnowflakeIngestionServiceV1) conn.buildIngestService(stageName, pipeName);
    try {
      HttpPost httpPostInsertRequest =
          ingestionService
              .getIngestManager()
              .getRequestBuilder()
              .generateInsertRequest(UUID.randomUUID(), pipeName, Collections.EMPTY_LIST, false);
      for (Header h : httpPostInsertRequest.getAllHeaders()) {
        if (h.getName().equalsIgnoreCase(HttpHeaders.USER_AGENT)) {
          System.out.println(h);
          Assertions.assertTrue(h.getValue().contains(userAgentExpectedSuffixInHttpHeader));
          Assertions.assertTrue(h.getValue().endsWith(userAgentExpectedSuffixInHttpHeader));
        }
      }
    } catch (Exception e) {
      Assertions.fail("Should not throw an exception:" + e.getMessage());
    }
  }

  @Test
  public void createConnectionService() {
    SnowflakeConnectionService service =
        SnowflakeConnectionServiceFactory.builder().setProperties(TestUtils.getConf()).build();

    assert service.getConnectorName().equals(TEST_CONNECTOR_NAME);

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0017,
        () -> {
          Map<String, String> conf = TestUtils.getConf();
          conf.remove(Utils.SF_URL);
          SnowflakeConnectionServiceFactory.builder().setProperties(conf).build();
        });

    SnowflakeURL url = TestUtils.getUrl();
    Properties prop = InternalUtils.createProperties(TestUtils.getConf(), url);
    String appName = TEST_CONNECTOR_NAME;

    service =
        SnowflakeConnectionServiceFactory.builder()
            .setProperties(prop)
            .setURL(url)
            .setConnectorName(appName)
            .build();

    assert service.getTelemetryClient() instanceof SnowflakeTelemetryServiceV1;

    assert service
        .getTelemetryClient()
        .getObjectNode()
        .get(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT)
        .toString()
        .equals("0");

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0003,
        () ->
            SnowflakeConnectionServiceFactory.builder()
                .setProperties(prop)
                .setConnectorName(appName)
                .build());

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0003,
        () ->
            SnowflakeConnectionServiceFactory.builder()
                .setURL(url)
                .setConnectorName(appName)
                .build());

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0003,
        () -> SnowflakeConnectionServiceFactory.builder().setURL(url).setProperties(prop).build());
  }

  @Test
  public void createConnectionService_SnowpipeStreaming() {

    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());

    SnowflakeConnectionService service =
        SnowflakeConnectionServiceFactory.builder().setProperties(config).build();

    assert service.getConnectorName().equals(TEST_CONNECTOR_NAME);

    assert service.getTelemetryClient() instanceof SnowflakeTelemetryServiceV2;

    assert service
        .getTelemetryClient()
        .getObjectNode()
        .get(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT)
        .toString()
        .equals("1");
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(tableName);
    conn.dropPipe(pipeName);
    conn.dropStage(stageName);
    TestUtils.dropTable(tableName1);
    conn.dropStage(stageName1);
  }

  @Test
  public void testTableFunctions() throws SQLException {
    // table doesn't exist
    assert !conn.tableExist(tableName);
    // create table
    conn.createTable(tableName);
    // table exists
    assert conn.tableExist(tableName);
    // insert some value
    TestUtils.executeQuery("insert into " + tableName + " values(123,123)");
    ResultSet resultSet = TestUtils.showTable(tableName);
    // value inserted
    assert InternalUtils.resultSize(resultSet) == 1;
    // create table if not exists
    conn.createTable(tableName);
    resultSet = TestUtils.showTable(tableName);
    // table hasn't been overwritten
    assert InternalUtils.resultSize(resultSet) == 1;
    // overwrite table
    conn.createTable(tableName, true);
    resultSet = TestUtils.showTable(tableName);
    // new table
    assert InternalUtils.resultSize(resultSet) == 0;
    // table is compatible
    assert conn.isTableCompatible(tableName);
    TestUtils.dropTable(tableName);
    // dropped table
    assert !conn.tableExist(tableName);
    // create incompatible table
    TestUtils.executeQuery("create table " + tableName + " (num int)");
    assert !conn.isTableCompatible(tableName);
    TestUtils.dropTable(tableName);
  }

  @Test
  public void testStageFunctions() {
    // stage doesn't exist
    assert !conn.stageExist(stageName);
    // create stage
    conn.createStage(stageName);
    // stage exists
    assert conn.stageExist(stageName);
    // put a file to stage
    String fileName =
        FileNameTestUtils.fileName(TEST_CONNECTOR_NAME, tableName, "topic", 1, 123, 456);
    conn.put(stageName, fileName, "test");
    // list stage with prefix
    List<String> files = conn.listStage(stageName, TEST_CONNECTOR_NAME);
    assert files.size() == 1;
    assert files.get(0).equals(fileName);
    // stage is compatible
    assert conn.isStageCompatible(stageName);
    // create stage if not exists
    conn.createStage(stageName);
    // stage hasn't been overwritten
    files = conn.listStage(stageName, "");
    assert files.size() == 1;
    // overwrite stage
    conn.createStage(stageName, true);
    files = conn.listStage(stageName, "");
    // empty stage
    assert files.size() == 0;
    // put incompatible file to stage
    String fileName1 = "123/adsasads.gz";
    conn.put(stageName, fileName1, "test");
    // stage is incompatible
    assert !conn.isStageCompatible(stageName);
    // drop stage if not empty
    assert !conn.dropStageIfEmpty(stageName);
    // stage hasn't been dropped
    assert conn.stageExist(stageName);
    // create table stage
    conn.createTable(tableName);
    // move file to table stage
    conn.moveToTableStage(tableName, stageName, "");
    // list table stage
    files = conn.listStage(tableName, "123", true);
    // file exits on table stage
    assert files.size() == 1;
    assert files.get(0).equals(fileName1);
    // drop table
    TestUtils.dropTable(tableName);
    // put two files to stage
    conn.put(stageName, fileName, "test");
    conn.put(stageName, fileName1, "test");
    // still not incompatible
    assert !conn.isStageCompatible(stageName);
    // list with prefix
    files = conn.listStage(stageName, TEST_CONNECTOR_NAME);
    // only one file
    assert files.size() == 1;
    assert files.get(0).equals(fileName);
    // move to table stage with name
    conn.createTable(tableName);
    List<String> files1 = new ArrayList<>(1);
    files1.add(fileName);
    conn.moveToTableStage(tableName, stageName, files1);
    // only one file on table stage
    files = conn.listStage(tableName, "", true);
    assert files.size() == 1;
    assert files.get(0).equals(fileName);
    // only one file on stage
    files = conn.listStage(stageName, "");
    assert files.size() == 1;
    assert files.get(0).equals(fileName1);
    // put one more file
    conn.put(stageName, fileName, "test");
    // two files on stage
    files = conn.listStage(stageName, "");
    assert files.size() == 2;
    // purge one file
    conn.purgeStage(stageName, files1);
    // one file on stage
    files = conn.listStage(stageName, "");
    assert files.size() == 1;
    assert files.get(0).equals(fileName1);
    conn.dropStage(stageName);
    conn.createStage(stageName);
    // drop if empty
    assert conn.dropStageIfEmpty(stageName);
    assert !conn.stageExist(stageName);
    TestUtils.dropTable(tableName);
  }

  @Test
  public void testStagePurgeFunctions() {
    // stage doesn't exist
    assert !conn.stageExist(stageName);
    // create stage
    conn.createStage(stageName);
    // stage exists
    assert conn.stageExist(stageName);
    // put two files to stage
    String fileName1 =
        FileNameTestUtils.fileName(TEST_CONNECTOR_NAME, tableName, topicName, 1, 1, 3);
    conn.put(stageName, fileName1, "test");
    String fileName2 =
        FileNameTestUtils.fileName(TEST_CONNECTOR_NAME, tableName, topicName, 1, 4, 6);
    conn.put(stageName, fileName2, "test");
    String fileName3 =
        FileNameTestUtils.fileName(TEST_CONNECTOR_NAME, tableName, topicName, 1, 14, 16);
    conn.put(stageName, fileName3, "test");
    String fileName4 =
        FileNameTestUtils.fileName(TEST_CONNECTOR_NAME, tableName, topicName, 1, 24, 26);
    conn.put(stageName, fileName4, "test");
    String fileName5 =
        FileNameTestUtils.fileName(TEST_CONNECTOR_NAME, tableName, topicName, 1, 34, 36);
    conn.put(stageName, fileName5, "test");
    String fileName6 =
        FileNameTestUtils.fileName(TEST_CONNECTOR_NAME, tableName, topicName, 1, 44, 46);
    conn.put(stageName, fileName6, "test");
    // list stage with prefix
    List<String> files = conn.listStage(stageName, TEST_CONNECTOR_NAME);
    assert files.size() == 6;

    List<String> filesList = new ArrayList<>();
    filesList.add(fileName1);
    filesList.add(fileName2);
    filesList.add(fileName3);
    filesList.add(fileName4);
    filesList.add(fileName5);
    filesList.add(fileName6);
    conn.purgeStage(stageName, filesList);

    files = conn.listStage(stageName, TEST_CONNECTOR_NAME);
    assert files.size() == 0;
  }

  @Test
  public void testPipeFunctions() {
    conn.createStage(stageName);
    conn.createTable(tableName);
    conn.createTable(tableName1);
    conn.createStage(stageName1);
    // pipe doesn't exit
    assert !conn.pipeExist(pipeName);
    // create pipe
    conn.createPipe(tableName, stageName, pipeName);
    // pipe exists
    assert conn.pipeExist(pipeName);
    // pipe is compatible
    assert conn.isPipeCompatible(tableName, stageName, pipeName);
    // pipe is incompatible with other table
    assert !conn.isPipeCompatible(tableName1, stageName, pipeName);
    // pipe is incompatible with other stage
    assert !conn.isPipeCompatible(tableName, stageName1, pipeName);
    // pipe hasn't been overwritten
    conn.createPipe(tableName1, stageName1, pipeName);
    assert !conn.isPipeCompatible(tableName1, stageName1, pipeName);
    // overwrite pipe
    conn.createPipe(tableName1, stageName1, pipeName, true);
    assert conn.isPipeCompatible(tableName1, stageName1, pipeName);
    // drop pipe
    conn.dropPipe(pipeName);
    assert !conn.pipeExist(pipeName);
  }

  @Test
  public void testTableCompatible() {
    TestUtils.executeQuery(
        "create or replace table "
            + tableName
            + "(record_content variant, record_metadata variant, other int)");
    assert conn.isTableCompatible(tableName);

    TestUtils.executeQuery(
        "create or replace table "
            + tableName
            + "(record_content variant, record_metadata string, other int)");
    assert !conn.isTableCompatible(tableName);

    TestUtils.executeQuery(
        "create or replace table "
            + tableName
            + "(record_content variant, abc variant, other int)");
    assert !conn.isTableCompatible(tableName);

    TestUtils.executeQuery(
        "create or replace table "
            + tableName
            + "(record_content variant, record_metadata variant, other int not null)");
    assert !conn.isTableCompatible(tableName);
  }

  @Test
  public void testConnectionFunction() {
    SnowflakeConnectionService service = TestUtils.getConnectionService();
    assert !service.isClosed();
    service.close();
    assert service.isClosed();
  }

  @Test
  public void testStreamingChannelOffsetMigration() {
    Map<String, String> testConfig = TestUtils.getConfForStreaming();
    SnowflakeConnectionService conn =
        SnowflakeConnectionServiceFactory.builder().setProperties(testConfig).build();
    conn.createTable(tableName);
    final String channelNameFormatV1 = SnowflakeSinkServiceV2.partitionChannelKey(tableName, 0);

    final String sourceChannelName =
        TopicPartitionChannel.generateChannelNameFormatV2(channelNameFormatV1, TEST_CONNECTOR_NAME);
    final String destinationChannelName = channelNameFormatV1;

    // ### TEST 1 - Both channels doesnt exist
    ChannelMigrateOffsetTokenResponseDTO channelMigrateOffsetTokenResponseDTO =
        conn.migrateStreamingChannelOffsetToken(
            tableName, sourceChannelName, destinationChannelName);
    Assertions.assertTrue(
        isChannelMigrationResponseSuccessful(channelMigrateOffsetTokenResponseDTO));
    Assertions.assertEquals(
        OFFSET_MIGRATION_SOURCE_CHANNEL_DOES_NOT_EXIST.getStatusCode(),
        channelMigrateOffsetTokenResponseDTO.getResponseCode());

    try {
      // ### TEST 2 - Table doesnt exist
      channelMigrateOffsetTokenResponseDTO =
          conn.migrateStreamingChannelOffsetToken(
              tableName + "_Table_DOESNT_EXIST", sourceChannelName, destinationChannelName);
    } catch (SnowflakeConnectionServiceV1.OffsetTokenMigrationRetryableException ex) {
      assert ex.getMessage()
          .contains(
              ChannelMigrationResponseCode.ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED.getMessage());
    }

    try {
      // ### TEST 3 - Source Channel (v2 channel doesnt exist)
      Map<String, String> config = TestUtils.getConfForStreaming();
      SnowflakeSinkConnectorConfig.setDefaultValues(config);
      TopicPartition topicPartition = new TopicPartition(tableName, 0);

      InMemorySinkTaskContext inMemorySinkTaskContext =
          new InMemorySinkTaskContext(Collections.singleton(topicPartition));

      // This will automatically create a channel for topicPartition.
      SnowflakeSinkService service =
          SnowflakeSinkServiceFactory.builder(
                  conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
              .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
              .setSinkTaskContext(inMemorySinkTaskContext)
              .addTask(tableName, topicPartition)
              .build();

      final long noOfRecords = 10;

      // send regular data
      List<SinkRecord> records =
          TestUtils.createJsonStringSinkRecords(0, noOfRecords, tableName, 0);

      service.insert(records);

      TestUtils.assertWithRetry(
          () -> service.getOffset(new TopicPartition(tableName, 0)) == noOfRecords, 5, 5);
      channelMigrateOffsetTokenResponseDTO =
          conn.migrateStreamingChannelOffsetToken(
              tableName, sourceChannelName, destinationChannelName);
      Assertions.assertTrue(
          isChannelMigrationResponseSuccessful(channelMigrateOffsetTokenResponseDTO));
      Assertions.assertEquals(
          OFFSET_MIGRATION_SOURCE_CHANNEL_DOES_NOT_EXIST.getStatusCode(),
          channelMigrateOffsetTokenResponseDTO.getResponseCode());

      // even after migration, it sends same offset from server side since source didnt exist
      TestUtils.assertWithRetry(
          () -> service.getOffset(new TopicPartition(tableName, 0)) == noOfRecords, 5, 5);

      // TEST 4: Do an actual migration from new channel format to old channel Format
      // Step 1: create a new source channel
      // Step 2: load some data
      // step 3: do a migration and check if destination channel has expected offset

      // Ctor of TopicPartitionChannel tries to open the channel.
      SnowflakeSinkServiceV2 snowflakeSinkServiceV2 = (SnowflakeSinkServiceV2) service;
      TopicPartitionChannel newChannelFormatV2 =
          new DirectTopicPartitionChannel(
              snowflakeSinkServiceV2.getStreamingIngestClient(),
              topicPartition,
              sourceChannelName,
              tableName,
              config,
              new InMemoryKafkaRecordErrorReporter(),
              new InMemorySinkTaskContext(Collections.singleton(topicPartition)),
              conn,
              conn.getTelemetryClient(),
              new SnowflakeSchemaEvolutionService(conn),
              new InsertErrorMapper());

      List<SinkRecord> recordsInChannelFormatV2 =
          TestUtils.createJsonStringSinkRecords(0, noOfRecords * 2, tableName, 0);
      for (int idx = 0; idx < recordsInChannelFormatV2.size(); idx++) {
        newChannelFormatV2.insertRecord(recordsInChannelFormatV2.get(idx), idx == 0);
      }

      TestUtils.assertWithRetry(
          () -> newChannelFormatV2.getOffsetSafeToCommitToKafka() == (noOfRecords * 2), 5, 5);

      conn.migrateStreamingChannelOffsetToken(tableName, sourceChannelName, destinationChannelName);
      TestUtils.assertWithRetry(
          () -> service.getOffset(new TopicPartition(tableName, 0)) == (noOfRecords * 2), 5, 5);
    } catch (Exception e) {
      Assertions.fail("Should not throw an exception:" + e.getMessage());
    } finally {
      TestUtils.dropTable(tableName);
    }
  }
}
