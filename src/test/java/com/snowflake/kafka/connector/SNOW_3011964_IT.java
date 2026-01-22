package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.internal.TestUtils.assertWithRetry;
import static com.snowflake.kafka.connector.internal.TestUtils.getTableContentOneRow;
import static com.snowflake.kafka.connector.internal.TestUtils.loadClasspathResource;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.internal.TestUtils;
import io.confluent.connect.avro.AvroConverter;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SNOW_3011964_IT extends ConnectClusterBaseIT {

  private static final String SCHEMA_SNOW_3011964 =
      loadClasspathResource("/com/snowflake/kafka/connector/avroschemas/schema3.json");
  public static final String TIMESTAMP_MILLIS = "TIMESTAMP_MILLIS";
  public static final String TIMESTAMP_LONG = "TIMESTAMP_LONG";

  private KafkaProducer<String, Object> avroProducer;

  @BeforeEach
  void beforeEach() {
    avroProducer = createAvroProducer();
  }

  @AfterEach
  void afterEach() {
    if (avroProducer != null) {
      avroProducer.close();
    }
  }

  @Test
  void test_SNOW_3011964_IT() throws Exception {
    // given
    snowflake.executeQueryWithParameters(
        String.format(
            "create table %s (%s TIMESTAMP(6), %s TIMESTAMP(6)) enable_schema_evolution = false",
            tableName, TIMESTAMP_MILLIS, TIMESTAMP_LONG));
    final Map<String, String> config = createConnectorConfig();
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    config.put("value.converter.schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when
    final Schema schema = new Schema.Parser().parse(SCHEMA_SNOW_3011964);
    final GenericRecord record = new GenericData.Record(schema);
    record.put(TIMESTAMP_MILLIS, 1768486316048L);
    record.put(TIMESTAMP_LONG, 1768486316048L);
    avroProducer.send(new ProducerRecord<>(topic0, "key1", record));
    avroProducer.flush();

    // then
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 1);
    Map<String, Object> firstRow = getTableContentOneRow(tableName);
    final Object avroLogicalTsAfterDbSave = firstRow.get(TIMESTAMP_MILLIS);
    final Object rawLongTsAfterDbSave = firstRow.get(TIMESTAMP_LONG);

    // properly saved and fetched because avro logical type for this is timestamp-millis.
    // Internally before we send this value to snowpipe SDK for saving
    // we're converting it into string (we detect it by looking at the schema).
    // When snowflake sees the destination column to be TIMESTAMP and
    // sent value is Sting number it converts it to propert timestamp
    assertThat(avroLogicalTsAfterDbSave)
        .isEqualTo(new Timestamp(1768486316048L)); // "2026-01-15 14:11:56.048"
    // Not saved properly. The value fetched from the database is x1000 greater.
    // This is because avro type for that value is long. It is being saved to database as long
    // when snowflake saves numeric long value to the column of type TIMESTAMP
    // it treats it as seconds (not milliseconds) and multiplies by 1000
    assertThat(rawLongTsAfterDbSave)
        .isEqualTo(new Timestamp(1768486316048L * 1000)); // "58011-02-06 06:54:08.0"
  }
}
