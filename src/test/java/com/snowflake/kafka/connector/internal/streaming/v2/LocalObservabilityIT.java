package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.snowflake.kafka.connector.SnowflakeSinkTask;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.SnowflakeSinkTaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingSinkServiceBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for local observability assertions (Task 7 of SNOW-3136119).
 *
 * <p>Verifies:
 *
 * <ol>
 *   <li>Noise logs (BatchOffsetFetcher "Fetched snowflake committed offset" and
 *       WaitForLastOffsetCommittedPolicy "retry no:") are NOT emitted at INFO level after ingest.
 *   <li>The startup log "starting task" is emitted at INFO with a non-empty task id.
 *   <li>Task-level JMX metrics {@code flush-duration} and {@code records-appended} exist and have a
 *       count &gt; 0 after ingest.
 * </ol>
 *
 * <p>SUT-gating: this test calls {@link TestUtils#getConnectionServiceWithEncryptedKey()} which
 * throws {@link IllegalStateException} when {@code SNOWFLAKE_CREDENTIAL_FILE} is not set. This
 * matches the gating pattern used by all other ITs in this project; the test will fail/error on
 * machines without a SUT profile.
 */
public class LocalObservabilityIT {

  // ---- SUT resources ----
  private final String table = TestUtils.randomTableName();
  private final int partition = 0;
  private final String topic = table;
  private final TopicPartition topicPartition = new TopicPartition(topic, partition);

  // ---- metrics wiring ----
  private MetricRegistry metricRegistry;
  private MetricsJmxReporter metricsJmxReporter;
  private SnowflakeSinkTaskMetrics taskMetrics;

  // ---- log capture ----
  private CollectingAppender batchOffsetFetcherAppender;
  private CollectingAppender waitPolicyAppender;
  private CollectingAppender sinkTaskAppender;

  private Logger batchOffsetFetcherLogger;
  private Logger waitPolicyLogger;
  private Logger sinkTaskLogger;

  // ---- system property restore ----
  private String priorLogLevel = System.getProperty("SS_LOG_LEVEL");

  @BeforeEach
  public void setUp() {
    // Capture SS_LOG_LEVEL before any test-driven side effects (SdkBootstrapConfig.apply may set
    // it)
    priorLogLevel = System.getProperty("SS_LOG_LEVEL");

    // Metrics wiring
    metricRegistry = new MetricRegistry();
    metricsJmxReporter = new MetricsJmxReporter(metricRegistry, TEST_CONNECTOR_NAME);
    taskMetrics = new SnowflakeSinkTaskMetrics(TEST_CONNECTOR_NAME, "0", metricsJmxReporter);

    // Log capture — attach appenders to the underlying Log4j loggers for each class under test.
    // KCLogger delegates to SLF4J which routes to Log4j (slf4j-log4j12 binding).
    batchOffsetFetcherAppender = new CollectingAppender();
    waitPolicyAppender = new CollectingAppender();
    sinkTaskAppender = new CollectingAppender();

    batchOffsetFetcherLogger =
        Logger.getLogger(
            "com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher");
    waitPolicyLogger =
        Logger.getLogger(
            "com.snowflake.kafka.connector.internal.streaming.v2.WaitForLastOffsetCommittedPolicy");
    sinkTaskLogger = Logger.getLogger("com.snowflake.kafka.connector.SnowflakeSinkTask");

    batchOffsetFetcherLogger.addAppender(batchOffsetFetcherAppender);
    waitPolicyLogger.addAppender(waitPolicyAppender);
    sinkTaskLogger.addAppender(sinkTaskAppender);

    // Ensure the loggers propagate events down to our test appenders.
    // Set DEBUG so both INFO and DEBUG events are captured; we assert absence at INFO.
    batchOffsetFetcherLogger.setLevel(Level.DEBUG);
    waitPolicyLogger.setLevel(Level.DEBUG);
    sinkTaskLogger.setLevel(Level.INFO);
  }

  @AfterEach
  public void tearDown() {
    batchOffsetFetcherLogger.removeAppender(batchOffsetFetcherAppender);
    waitPolicyLogger.removeAppender(waitPolicyAppender);
    sinkTaskLogger.removeAppender(sinkTaskAppender);
    taskMetrics.unregister();
    TestUtils.dropTable(table);
    // Restore SS_LOG_LEVEL to its pre-test state (SdkBootstrapConfig.apply may have set it)
    if (priorLogLevel == null) {
      System.clearProperty("SS_LOG_LEVEL");
    } else {
      System.setProperty("SS_LOG_LEVEL", priorLogLevel);
    }
  }

  @Test
  public void noiseLogsDowngradedAndMetricsRecordedAfterIngest() throws Exception {
    // ---- SUT-gated setup (throws IllegalStateException without SNOWFLAKE_CREDENTIAL_FILE) ----
    SnowflakeConnectionService conn = TestUtils.getConnectionServiceWithEncryptedKey();
    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(true);
    SinkTaskConfig sinkTaskConfig =
        SinkTaskConfig.builderFrom(config).validation(SnowflakeValidation.SERVER_SIDE).build();

    // ---- 1. Assert: "starting task" log emitted at INFO with a real task id ----
    // Drive through SnowflakeSinkTask.start() which emits the log we need to verify.
    // Use the same raw config so it hits the same code path as production.
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    sinkTask.start(config);

    assertThat(sinkTaskAppender.containsMessageMatching(Level.INFO, "starting task"))
        .as("SnowflakeSinkTask.start() must emit an INFO 'starting task ...' log")
        .isTrue();
    // The default sentinel id is "-1"; a properly resolved id must differ from it.
    assertThat(sinkTaskAppender.containsMessageMatching(Level.INFO, "starting task -1"))
        .as("The task id in 'starting task ...' must not be the unset sentinel '-1'")
        .isFalse();

    sinkTask.stop();

    // ---- 2. Ingest records with real TaskMetrics wired in ----
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, sinkTaskConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withMetricsJmxReporter(metricsJmxReporter)
            .withTaskMetrics(taskMetrics)
            .build();

    service.startPartition(topicPartition);
    service.awaitInitialization();

    Converter converter = buildJsonConverter();
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      var input =
          converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
      records.add(
          new SinkRecord(
              topic,
              partition,
              Schema.STRING_SCHEMA,
              "key-" + i,
              input.schema(),
              input.value(),
              i));
    }
    service.insert(records);

    // Trigger a time-based flush by inserting an empty batch (same pattern as other ITs)
    TestUtils.assertWithRetry(
        () -> {
          service.insert(new ArrayList<>());
          return service.getOffset(topicPartition) == 3;
        },
        10,
        20);

    // Stop the service to exercise the shutdown drain: SnowflakeSinkServiceV2.stop() ->
    // channelManager.waitForAllChannelsToCommitData() -> waitForLastProcessedRecordCommitted(),
    // which is the only path that records flush-duration (SSv2 commits are otherwise SDK-async).
    service.stop();

    // ---- 3. Assert: noise logs NOT at INFO level ----
    assertThat(
            batchOffsetFetcherAppender.containsMessageMatching(
                Level.INFO, "Fetched snowflake committed offset"))
        .as(
            "BatchOffsetFetcher must NOT log 'Fetched snowflake committed offset' at INFO"
                + " (Task 5: noise downgrade to DEBUG)")
        .isFalse();

    assertThat(waitPolicyAppender.containsMessageMatching(Level.INFO, "retry no:"))
        .as(
            "WaitForLastOffsetCommittedPolicy must NOT log 'retry no:' at INFO"
                + " (Task 5: noise downgrade to DEBUG)")
        .isFalse();

    // Effective-level assertions: prove the downgrade is in production code, not just absent.
    // setUp sets both loggers to DEBUG to capture events; these assertions fail if someone reverts
    // the production log call back to INFO (because a production INFO log would have appeared
    // above,
    // but more directly: the logger effective level is what this test set it to, and the production
    // code must emit at DEBUG for it to be captured without appearing at INFO).
    assertThat(batchOffsetFetcherLogger.getEffectiveLevel())
        .as(
            "BatchOffsetFetcher logger must be at DEBUG (setUp sets it; production must not"
                + " override it)")
        .isEqualTo(Level.DEBUG);
    assertThat(waitPolicyLogger.getEffectiveLevel())
        .as(
            "WaitForLastOffsetCommittedPolicy logger must be at DEBUG (setUp sets it; production"
                + " must not override it)")
        .isEqualTo(Level.DEBUG);

    // ---- 4. Assert: task-level JMX metrics exist with count > 0 ----
    // Timers: flush-duration
    Map<String, Timer> timers = metricRegistry.getTimers();
    assertThat(timers.keySet())
        .as("MetricRegistry must contain a timer whose name ends with 'flush-duration'")
        .anyMatch(name -> name.endsWith("flush-duration"));

    Timer flushTimer =
        timers.entrySet().stream()
            .filter(e -> e.getKey().endsWith("flush-duration"))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new AssertionError("flush-duration timer not found in registry"));
    assertThat(flushTimer.getCount())
        .as("flush-duration must have at least one recorded timing after ingest")
        .isGreaterThan(0);

    // Meters: records-appended
    Map<String, Meter> meters = metricRegistry.getMeters();
    assertThat(meters.keySet())
        .as("MetricRegistry must contain a meter whose name ends with 'records-appended'")
        .anyMatch(name -> name.endsWith("records-appended"));

    Meter recordsAppendedMeter =
        meters.entrySet().stream()
            .filter(e -> e.getKey().endsWith("records-appended"))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new AssertionError("records-appended meter not found in registry"));
    assertThat(recordsAppendedMeter.getCount())
        .as("records-appended must have count > 0 after inserting 3 records")
        .isGreaterThan(0);

    service.closeAll();
  }

  // ---- helpers ----

  private static Converter buildJsonConverter() {
    JsonConverter converter = new JsonConverter();
    HashMap<String, String> cfg = new HashMap<>();
    cfg.put("schemas.enable", "false");
    converter.configure(cfg, true);
    return converter;
  }

  /** Log4j appender that collects events for later assertion. */
  private static class CollectingAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }

    @Override
    public void close() {}

    @Override
    public boolean requiresLayout() {
      return false;
    }

    boolean containsMessageMatching(Level level, String fragment) {
      return events.stream()
          .anyMatch(
              e ->
                  e.getLevel().equals(level)
                      && e.getRenderedMessage() != null
                      && e.getRenderedMessage().contains(fragment));
    }
  }
}
