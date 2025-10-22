package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
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
import com.snowflake.kafka.connector.internal.streaming.StreamingSinkServiceBuilder;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake.SnowflakeSchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectionServiceIT {
  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();

  private final String tableName = TestUtils.randomTableName();
  private final String tableName1 = TestUtils.randomTableName();
  private final String topicName = TestUtils.randomTopicName();

  @Test
  void testEncryptedKey() {
    // no exception
    SnowflakeConnectionServiceFactory.builder()
        .setProperties(TestUtils.getConfWithEncryptedKey())
        .build();
  }

  @Test
  void testOAuthAZ() {
    Map<String, String> confWithOAuth = TestUtils.getConfWithOAuth();
    assert confWithOAuth.containsKey(Utils.SF_OAUTH_CLIENT_ID);
    SnowflakeConnectionServiceFactory.builder().setProperties(TestUtils.getConfWithOAuth()).build();
  }

  @Test
  void testSetSSLProperties() {
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
  void createConnectionService_SnowpipeStreaming() {

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
        .equals("0"); // SNOWPIPE_STREAMING is now ordinal 0
  }

  @AfterEach
  void afterEach() {
    TestUtils.dropTable(tableName);
    TestUtils.dropTable(tableName1);
  }

  @Test
  void testTableFunctions() throws SQLException {
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
  void testConnectionFunction() {
    SnowflakeConnectionService service = TestUtils.getConnectionService();
    assert !service.isClosed();
    service.close();
    assert service.isClosed();
  }

  @Test
  void testStreamingChannelOffsetMigration() {
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

      // This will automatically create a channel for topicPartition.
      SnowflakeSinkServiceV2 service =
          StreamingSinkServiceBuilder.builder(conn, config)
              .withSinkTaskContext(
                  new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
              .build();
      service.startPartition(tableName, topicPartition);

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
      TopicPartitionChannel newChannelFormatV2 =
          new DirectTopicPartitionChannel(
              service.getStreamingIngestClient(),
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

