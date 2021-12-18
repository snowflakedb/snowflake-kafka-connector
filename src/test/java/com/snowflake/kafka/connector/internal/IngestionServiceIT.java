package com.snowflake.kafka.connector.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.connection.ClientStatusResponse;
import net.snowflake.ingest.connection.ConfigureClientResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IngestionServiceIT {

  private SnowflakeIngestionService ingestService = null;
  private final SnowflakeConnectionService conn =
      SnowflakeConnectionServiceFactory.builder().setProperties(TestUtils.getConf()).build();
  private String stage = TestUtils.randomStageName();
  private String pipe = TestUtils.randomPipeName();
  private String table = TestUtils.randomTableName();

  @Before
  public void before() {
    conn.createStage(stage);
    conn.createTable(table);
    conn.createPipe(table, stage, pipe);
    ingestService = conn.buildIngestService(stage, pipe);
  }

  @After
  public void after() {
    conn.dropPipe(pipe);
    conn.dropStage(stage);
    TestUtils.dropTable(table);
  }

  @Test
  public void ingestFileTest() throws Exception {
    String file = "{\"aa\":123}";
    // File Name Format: app/table/partition/start_end_timeStamp.fileFormat.gz
    // start offset = 0, end offset = 1
    String fileName = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, 0, 0, 1);

    conn.put(stage, fileName, file);
    ingestService.ingestFile(fileName);
    List<String> names = new ArrayList<>(1);
    names.add(fileName);
    // ingest report
    TestUtils.assertWithRetry(
        () -> {
          Map<String, InternalUtils.IngestedFileStatus> result =
              ingestService.readIngestReport(names);
          return result.get(fileName).equals(InternalUtils.IngestedFileStatus.LOADED);
        },
        60,
        9);
    // load history
    TestUtils.assertWithRetry(
        () -> {
          Map<String, InternalUtils.IngestedFileStatus> result =
              ingestService.readOneHourHistory(names, System.currentTimeMillis() - 3600 * 1000);
          System.out.println(result.get(fileName));
          return result.get(fileName).equals(InternalUtils.IngestedFileStatus.LOADED);
        },
        15,
        4);

    assert ingestService.getStageName().equals(stage);
  }

  @Test
  public void ingestFileWithClientInfoTestSuccessful() throws Exception {
    String file = "{\"aa\":123}";
    // File Name Format: app/table/partition/start_end_timeStamp.fileFormat.gz
    // start offset = 0, end offset = 1
    String fileName = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, 0, 0, 1);
    // Upload a file on internal stage
    conn.put(stage, fileName, file);

    // Register/Configure a snowpipe client
    ConfigureClientResponse configureClientResponse = ingestService.configureClient();
    assert configureClientResponse.getClientSequencer().equals(0l);
    // Reconfigure the snowpipe client, the clientSequencer should increase
    configureClientResponse = ingestService.configureClient();
    assert configureClientResponse.getClientSequencer().equals(1l);
    // Get the client offset token
    ClientStatusResponse clientStatusResponse = ingestService.getClientStatus();
    assert clientStatusResponse.getOffsetToken() == null;
    // Ingest the file with client info
    ingestService.ingestFilesWithClientInfo(
        new ArrayList<>(Arrays.asList(fileName)), configureClientResponse.getClientSequencer());
    // recreate the file list for ingest report
    List fileNameList = new ArrayList<>(Arrays.asList(fileName));
    // make sure ingest is successful
    TestUtils.assertWithRetry(
        () -> {
          Map<String, InternalUtils.IngestedFileStatus> result =
              ingestService.readIngestReport(fileNameList);
          return result.get(fileName).equals(InternalUtils.IngestedFileStatus.LOADED);
        },
        60,
        9);
    // after ingestion, check if the offset token is updated
    clientStatusResponse = ingestService.getClientStatus();
    assert clientStatusResponse.getOffsetToken().equals("1");
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void ingestFileWithClientInfoTestFailed() throws Exception {
    String file = "{\"aa\":123}";
    // File Name Format: app/table/partition/start_end_timeStamp.fileFormat.gz
    // start offset = 0, end offset = 1
    String fileName = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, 0, 0, 1);
    // Upload a file on internal stage
    conn.put(stage, fileName, file);
    // Register/Configure a snowpipe client
    ConfigureClientResponse configureClientResponse = ingestService.configureClient();
    assert configureClientResponse.getClientSequencer().equals(0l);
    // Reconfigure the snowpipe client, the clientSequencer should increase
    configureClientResponse = ingestService.configureClient();
    assert configureClientResponse.getClientSequencer().equals(1l);
    // Get the client offset token
    ClientStatusResponse clientStatusResponse = ingestService.getClientStatus();
    assert clientStatusResponse.getOffsetToken() == null;
    // Ingest the file with outdated client info should throw error
    ingestService.ingestFilesWithClientInfo(new ArrayList<>(Arrays.asList(fileName)), 0l);
  }

  @Test
  public void ingestMultipleFilesWithClientInfoTest() {
    String fileContent = "{\"aa\":123}";
    // start offset = 0, end offset = 1
    String fileName1 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, 0, 0, 1);
    // start offset = 4, end offset = 5
    String fileName2 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, 0, 4, 5);
    // start offset = 2, end offset = 3
    String fileName3 = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, 0, 2, 3);
    // Register/Configure a snowpipe client
    ConfigureClientResponse configureClientResponse = ingestService.configureClient();
    assert configureClientResponse.getClientSequencer().equals(0l);
    // Get the client offset token
    ClientStatusResponse clientStatusResponse = ingestService.getClientStatus();
    assert clientStatusResponse.getOffsetToken() == null;
    // Ingest files that are not sequential, offset should update to latest
    ingestService.ingestFilesWithClientInfo(
        new ArrayList<>(Arrays.asList(fileName1, fileName2, fileName3)), 0l);
    // Offset should update to 3
    clientStatusResponse = ingestService.getClientStatus();
    assert clientStatusResponse.getOffsetToken().equals("5");
  }
}
