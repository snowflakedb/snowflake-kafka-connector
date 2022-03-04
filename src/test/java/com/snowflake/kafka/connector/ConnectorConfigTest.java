package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfig;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.Locale;
import java.util.Map;
import org.junit.Test;

public class ConnectorConfigTest {

  @Test
  public void testConfig() {
    Map<String, String> config = getConfig();
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyFlushTime() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testFlushTimeSmall() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        (SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN - 1) + "");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testFlushTimeNotNumber() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "fdas");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyName() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.NAME);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testURL() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyUser() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyDatabase() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptySchema() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyPrivateKey() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY);
    Utils.validateConfig(config);
  }

  @Test
  public void testCorrectProxyHost() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "127.0.0.1");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyPort() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "127.0.0.1");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyHost() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIllegalTopicMap() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "$@#$#@%^$12312");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIllegalTableName() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "topic1:!@#@!#!@");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testDuplicatedTopic() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "topic1:table1,topic1:table2");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testDuplicatedTableName() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "topic1:table1,topic2:table1");
    Utils.validateConfig(config);
  }

  @Test
  public void testNameMapCovered() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS, "!@#,$%^,test");
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "!@#:table1,$%^:table2");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testBufferSizeRange() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN - 1 + "");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testBufferSizeValue() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, "afdsa");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyBufferSize() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyBufferCount() {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyBufferCountNegative() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "-1");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testBufferCountValue() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "adssadsa");
    Utils.validateConfig(config);
  }

  @Test
  public void testKafkaProviderConfigValue_valid_null() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, null);
    Utils.validateConfig(config);
  }

  @Test
  public void testKafkaProviderConfigValue_valid_empty() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "");
    Utils.validateConfig(config);
  }

  @Test
  public void testKafkaProviderConfigValue_valid_provider() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "self_hosted");
    Utils.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "CONFLUENT");
    Utils.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "UNKNOWN");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testKafkaProviderConfigValue_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG, "Something_which_is_not_supported");
    Utils.validateConfig(config);
  }

  @Test
  public void testBehaviorOnNullValuesConfig_valid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "IGNORE");
    Utils.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "DEFAULT");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testBehaviorOnNullValuesConfig_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "INVALID");
    Utils.validateConfig(config);
  }

  @Test
  public void testJMX_valid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JMX_OPT, "true");
    Utils.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.JMX_OPT, "False");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testJMX_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JMX_OPT, "INVALID");
    Utils.validateConfig(config);
  }

  @Test
  public void testDeliveryGuarantee_valid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE, "at_least_once");
    Utils.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE, "exactly_once");
    Utils.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE, "");
    Utils.validateConfig(config);

    config.put(SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE, null);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testDeliveryGuarantee_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE, "INVALID");
    Utils.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_valid_value_snowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE.toString());
    Utils.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_valid_value_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIngestionTypeConfig_invalid_snowpipe_streaming() {
    Map<String, String> config = getConfig();

    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIngestionTypeConfig_invalid_value() {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT, "INVALID_VALUE");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIngestionTypeConfig_streaming_invalid_delivery_guarantee() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE,
        SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee.AT_LEAST_ONCE.name());
    Utils.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_streaming_valid_delivery_guarantee() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString().toUpperCase(Locale.ROOT));
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE,
        SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee.EXACTLY_ONCE.name());
    Utils.validateConfig(config);
  }

  @Test
  public void testIngestionTypeConfig_streaming_default_delivery_guarantee() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    Utils.validateConfig(config);
  }

  /** These error tests are not going to enforce errors if they are not passed as configs. */
  @Test
  public void testErrorTolerance_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    Utils.validateConfig(config);

    config.put(
        ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.NONE.toString());
    Utils.validateConfig(config);

    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testErrorTolerance_DisallowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_TOLERANCE_CONFIG, "INVALID");
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    Utils.validateConfig(config);
  }

  @Test
  public void testErrorLog_AllowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_LOG_ENABLE_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    Utils.validateConfig(config);

    config.put(ERRORS_LOG_ENABLE_CONFIG, "FALSE");
    Utils.validateConfig(config);

    config.put(ERRORS_LOG_ENABLE_CONFIG, "TRUE");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testErrorLog_DisallowedValues() {
    Map<String, String> config = getConfig();
    config.put(ERRORS_LOG_ENABLE_CONFIG, "INVALID");
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    Utils.validateConfig(config);
  }

  // ---------- Streaming Buffer tests ---------- //
  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testStreamingEmptyFlushTime() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testStreamingFlushTimeSmall() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        (StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC - 1) + "");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testStreamingFlushTimeNotNumber() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "fdas");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testStreamingEmptyBufferSize() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testStreamingEmptyBufferCount() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testStreamingBufferCountNegative() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "-1");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testStreamingBufferCountValue() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "adssadsa");
    Utils.validateConfig(config);
  }

  @Test
  public void testValidKeyAndValueConvertersForStreamingSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    Utils.validateConfig(config);

    config.put(
        SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    Utils.validateConfig(config);

    config.put(
        SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD,
        "io.confluent.connect.avro.AvroConverter");
    Utils.validateConfig(config);

    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    Utils.validateConfig(config);

    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    Utils.validateConfig(config);

    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "io.confluent.connect.avro.AvroConverter");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testInvalidKeyConvertersForStreamingSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD,
        "com.snowflake.kafka.connector.records.SnowflakeJsonConverter");

    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testInvalidValueConvertersForStreamingSnowpipe() {
    Map<String, String> config = getConfig();
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "ACCOUNTADMIN");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "com.snowflake.kafka.connector.records.SnowflakeJsonConverter");

    config.put(
        SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.storage.StringConverter");
    Utils.validateConfig(config);
  }
}
