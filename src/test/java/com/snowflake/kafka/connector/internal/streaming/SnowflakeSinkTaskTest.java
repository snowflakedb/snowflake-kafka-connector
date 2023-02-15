package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkTask;
import com.snowflake.kafka.connector.SnowflakeSinkTaskTestForStreamingIT;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.ingestsdk.IngestSdkProvider;
import com.snowflake.kafka.connector.internal.ingestsdk.StreamingClientManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;

public class SnowflakeSinkTaskTest {

  // JUST FOR LOGGING TESTING, these should not be used anywhere else
  @Mock private Logger logger = Mockito.mock(Logger.class);

  @InjectMocks @Spy
  private LoggerHandler loggerHandler = Mockito.spy(new LoggerHandler(this.getClass().getName()));

  @InjectMocks private SnowflakeSinkTask sinkTask1 = new SnowflakeSinkTask();

  @Ignore
  @Test
  public void testMultipleSinkTaskWithLogs() throws Exception {
    // setup task0, the real one
    int taskId0 = 0;
    String topicName0 = "topicName0";
    SnowflakeSinkTask sinkTask0 = new SnowflakeSinkTask();
    Map<String, String> config0 = SnowflakeSinkTaskTestForStreamingIT.getConfig(taskId0);
    List<TopicPartition> topicPartitions0 =
        SnowflakeSinkTaskTestForStreamingIT.getTopicPartitions(topicName0, 1);
    InMemorySinkTaskContext sinkTaskContext0 =
        new InMemorySinkTaskContext(Collections.singleton(topicPartitions0.get(0)));
    List<SinkRecord> records0 = TestUtils.createJsonStringSinkRecords(0, 1, topicName0, 0);
    Map<TopicPartition, OffsetAndMetadata> offsetMap0 = new HashMap<>();
    offsetMap0.put(topicPartitions0.get(0), new OffsetAndMetadata(10000));

    // set up task1, the !real one (logging verification one)
    // basically the same as sinktask0 except its logger is mocked
    int taskId1 = 1;
    String topicName1 = "topicName1";
    Map<String, String> config1 = SnowflakeSinkTaskTestForStreamingIT.getConfig(taskId1);
    List<TopicPartition> topicPartitions1 =
        SnowflakeSinkTaskTestForStreamingIT.getTopicPartitions(topicName1, 1);
    InMemorySinkTaskContext sinkTaskContext1 =
        new InMemorySinkTaskContext(Collections.singleton(topicPartitions1.get(0)));
    List<SinkRecord> records1 = TestUtils.createJsonStringSinkRecords(0, 1, topicName1, 0);
    Map<TopicPartition, OffsetAndMetadata> offsetMap1 = new HashMap<>();
    offsetMap1.put(topicPartitions1.get(0), new OffsetAndMetadata(10000));
    // task1 logging
    int task1OpenCount = 0;
    MockitoAnnotations.initMocks(this);
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    String expectedTask1Tag =
        TestUtils.getExpectedLogTagWithoutCreationCount(taskId1 + "", task1OpenCount);
    Mockito.doCallRealMethod().when(loggerHandler).setLoggerInstanceTag(expectedTask1Tag);

    // set up two clients
    IngestSdkProvider.setStreamingClientManager(new StreamingClientManager(new HashMap<>()));
    // TODO @rcheng: use jay's reset method when merged
    IngestSdkProvider.getStreamingClientManager().createAllStreamingClients(config0, "kcid", 2, 1);
    assert IngestSdkProvider.getStreamingClientManager().getClientCount() == 2;

    // init tasks
    sinkTask0.initialize(sinkTaskContext0);
    sinkTask1.initialize(sinkTaskContext1);

    // start tasks
    sinkTask0.start(config0);
    sinkTask1.start(config1);

    // verify task1 start logs
    Mockito.verify(loggerHandler, Mockito.times(1))
        .setLoggerInstanceTag(Mockito.contains(expectedTask1Tag));
    Mockito.verify(logger, Mockito.times(2))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("start")));

    // open tasks
    sinkTask0.open(topicPartitions0);
    sinkTask1.open(topicPartitions1);

    // verify task1 open logs
    task1OpenCount++;
    expectedTask1Tag =
        TestUtils.getExpectedLogTagWithoutCreationCount(taskId1 + "", task1OpenCount);
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("open")));

    // send data to tasks
    sinkTask0.put(records0);
    sinkTask1.put(records1);

    // verify task1 put logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("put")));

    // commit offsets
    TestUtils.assertWithRetry(() -> sinkTask0.preCommit(offsetMap0).size() == 1, 20, 5);
    TestUtils.assertWithRetry(() -> sinkTask1.preCommit(offsetMap1).size() == 1, 20, 5);

    // verify task1 precommit logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(
                Mockito.contains(expectedTask1Tag), Mockito.contains("precommit")));

    TestUtils.assertWithRetry(
        () -> sinkTask0.preCommit(offsetMap0).get(topicPartitions0.get(0)).offset() == 1, 20, 5);
    TestUtils.assertWithRetry(
        () -> sinkTask1.preCommit(offsetMap1).get(topicPartitions1.get(0)).offset() == 1, 20, 5);

    // close tasks
    sinkTask0.close(topicPartitions0);
    sinkTask1.close(topicPartitions1);

    // verify task1 close logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("closed")));

    // stop tasks
    sinkTask0.stop();
    sinkTask1.stop();

    // verify task1 stop logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("stop")));
  }
}
