package com.snowflake.kafka.connector.internal.metrics;

import java.util.Collection;
import java.util.Map;

/** All metrics related constants. Mainly for JMX */
public class MetricsUtil {
  public static final String JMX_METRIC_PREFIX = "snowflake.kafka.connector";

  // Offset related constants
  public static final String FILE_COUNT_SUB_DOMAIN = "file-counts";

  /**
   * Number of files we call insertFiles API in snowpipe. Note: There is currently a limitation of
   * 5k files being sent to a single rest request. So these metric has no one to one relation
   * between files and number of REST API calls. Number of REST API call for insertFiles can be
   * larger than this value. The value drop backs to zero if there are no more files to be ingested.
   */
  public static final String FILE_COUNT_ON_INGESTION = "file-count-on-ingestion";

  /**
   * Number of files currently on an internal stage. The value will be decremented once files are
   * being purged. These gives an estimate on how many files are present on an internal stage at any
   * given point of time. (The value is decremented if purged few files due to ingestion status)
   */
  public static final String FILE_COUNT_ON_STAGE = "file-count-on-stage";

  /**
   * Number of files purged from internal stage because we were able to verify the ingestion status
   */
  public static final String FILE_COUNT_PURGED = "file-count-purged";

  /**
   * Number of files present on table stage because files corresponds to broken offset (Broken
   * record)
   */
  public static final String FILE_COUNT_TABLE_STAGE_BROKEN_RECORD =
      "file-count-table-stage-broken-record";

  /** Number of files present on table stage due to failed ingestion (Missing Ingestion Status). */
  public static final String FILE_COUNT_TABLE_STAGE_INGESTION_FAIL =
      "file-count-table-stage-ingestion-fail";

  // file count related constants
  public static final String OFFSET_SUB_DOMAIN = "offsets";
  /**
   * Offset number that is most recent inside the buffer (In memory buffer)
   *
   * <p>This is updated every time an offset is sent as put API of SinkTask {@link
   * org.apache.kafka.connect.sink.SinkTask#put(Collection)}
   */
  public static final String PROCESSED_OFFSET = "processed-offset";

  /**
   * Offset number(Record) that is being flushed into an internal stage after the buffer threshold
   * was reached. Buffer can reach its threshold by either time, number of records or size.
   */
  public static final String FLUSHED_OFFSET = "flushed-offset";

  /**
   * Offset number (Record) for which precommit {@link
   * org.apache.kafka.connect.sink.SinkTask#preCommit(Map)} API was called and we called snowpipe's
   * insertFiles API.
   */
  public static final String COMMITTED_OFFSET = "committed-offset";

  /**
   * Offsets which are being purged from internal stage. (This number is the highest recent most
   * offset which was purged from internal stage)
   */
  public static final String PURGED_OFFSET = "purged-offset";

  // Buffer related constants
  public static final String BUFFER_SUB_DOMAIN = "buffer";

  // the inmemory buffer size in bytes
  public static final String BUFFER_SIZE_BYTES = "buffer-size-bytes";

  // in memory buffer count representing the number of records in kafka
  public static final String BUFFER_RECORD_COUNT = "buffer-record-count";

  // Event Latency related constants

  public static final String LATENCY_SUB_DOMAIN = "latencies";

  // ************ Streaming Constants ************//
  /**
   * See {@link com.snowflake.kafka.connector.internal.streaming.DirectTopicPartitionChannel} for
   * offset description
   */
  public static final String OFFSET_PERSISTED_IN_SNOWFLAKE = "persisted-in-snowflake-offset";

  public static final String LATEST_CONSUMER_OFFSET = "latest-consumer-offset";
  // ********** ^ Streaming Constants ^ **********//

  public enum EventType {
    /**
     * Time difference between the record put into kafka to record fetched into Kafka Connector Can
     * be null if the value was not set inside a record. {@link
     * org.apache.kafka.connect.connector.ConnectRecord#timestamp()}
     */
    KAFKA_LAG("kafka-lag"),

    /**
     * Time difference between file being uploaded to internal stage to time invoking insertFiles
     * API.
     */
    COMMIT_LAG("commit-lag"),

    /**
     * Time difference between file being uploaded to internal stage to time knowing the successful
     * file ingestion status through insertReport or loadHistoryScan API.
     */
    INGESTION_LAG("ingestion-lag"),
    ;

    /** The metric name that will be used in JMX */
    private final String metricName;

    EventType(final String metricName) {
      this.metricName = metricName;
    }

    public String getMetricName() {
      return this.metricName;
    }
  }

  /**
   * Construct the actual metrics name that will be passed in by dropwizard framework to {@link
   * MetricsJmxReporter#getObjectName(String, String, String)} We will prefix actual metric name
   * with partitionName and subcategory of the metric.
   *
   * <p>Will be of form <b>partitionName/subDomain/metricName</b>
   *
   * @param partitionName partitionNAme based on partition number (pipeName for Snowpipe or
   *     partitionChannelKey for Streaming)
   * @param subDomain categorize this metric (Actual ObjectName creation Logic will be handled in
   *     getObjectName)
   * @param metricName actual Metric name for which we will use Gauge, Meter, Histogram
   * @return concatenized String
   */
  public static String constructMetricName(
      final String partitionName, final String subDomain, final String metricName) {
    return String.format("%s/%s/%s", partitionName, subDomain, metricName);
  }
}
