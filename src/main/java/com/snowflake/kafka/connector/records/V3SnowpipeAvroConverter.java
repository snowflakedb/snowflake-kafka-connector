package com.snowflake.kafka.connector.records;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * A sink-only {@code value.converter} for Confluent Schema Registry Avro that produces the record
 * value in the JSON shape the Kafka connector 3.x {@code SnowflakeAvroConverter} wrote to {@code
 * RECORD_CONTENT} in Snowpipe mode. Records are returned without a Connect schema, so they are
 * stored as a single {@code RECORD_CONTENT} document and require {@code
 * snowflake.enable.schematization=false}.
 *
 * <p>Supports the optional {@value #READER_SCHEMA_CONFIG} property: an Avro schema that every
 * record is resolved to (projection, defaults, aliases, and type promotion) before conversion.
 * Schema Registry connection, authentication, and TLS settings are the standard Confluent
 * deserializer properties.
 */
public final class V3SnowpipeAvroConverter implements Converter {

  /** Optional Avro reader schema, as JSON, applied to every record. */
  public static final String READER_SCHEMA_CONFIG = "reader.schema";

  private final SchemaRegistryClient registryOverride;
  private KafkaAvroDeserializer deserializer;
  private Schema readerSchema;

  /** Creates a converter that builds its Schema Registry client from its configuration. */
  public V3SnowpipeAvroConverter() {
    this(null);
  }

  /** Creates a converter that reads schemas from {@code registry}. */
  V3SnowpipeAvroConverter(SchemaRegistryClient registry) {
    this.registryOverride = registry;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.readerSchema = parseReaderSchema(configs.get(READER_SCHEMA_CONFIG));
    Map<String, Object> deserializerConfigs = withDefaults(configs);
    deserializerConfigs.remove(READER_SCHEMA_CONFIG);
    // Generic records with raw logical-type values are the input the value mapper expects.
    deserializerConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
    deserializerConfigs.put(
        KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, false);
    this.deserializer =
        registryOverride == null
            ? new KafkaAvroDeserializer()
            : new KafkaAvroDeserializer(registryOverride);
    this.deserializer.configure(deserializerConfigs, isKey);
  }

  /**
   * Returns a copy of {@code configs} with this converter's defaults applied. User-set values win.
   *
   * <p>Disables Confluent's ServiceLoader discovery of Data Contracts rule executors ({@code
   * rule.service.loader.enable}). On Confluent Platform workers the rule executor implementations
   * on the worker's base classpath are loaded by a different classloader than the Confluent classes
   * bundled with this connector, and configuration fails with "not a subtype".
   */
  static Map<String, Object> withDefaults(Map<String, ?> configs) {
    Map<String, Object> withDefaults = new HashMap<>(configs);
    withDefaults.putIfAbsent(AbstractKafkaSchemaSerDeConfig.RULE_SERVICE_LOADER_ENABLE, false);
    return withDefaults;
  }

  /**
   * Converts a Confluent-framed Avro payload. A null payload is a tombstone. Malformed payloads,
   * unknown schema ids, Schema Registry failures, and records that can't be resolved to the reader
   * schema throw {@link DataException}.
   */
  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    if (value == null) {
      return SchemaAndValue.NULL;
    }
    Object datum;
    try {
      datum =
          readerSchema == null
              ? deserializer.deserialize(topic, headers, value)
              : deserializer.deserialize(topic, headers, value, readerSchema);
    } catch (RuntimeException e) {
      throw new DataException("Failed to deserialize Avro data for topic " + topic, e);
    }
    if (datum == null) {
      return SchemaAndValue.NULL;
    }
    Schema schema =
        datum instanceof GenericContainer ? ((GenericContainer) datum).getSchema() : null;
    return new SchemaAndValue(null, V3SnowpipeAvroValueMapper.toValue(schema, datum));
  }

  /** Always throws: this converter only reads records. */
  @Override
  public byte[] fromConnectData(
      String topic, org.apache.kafka.connect.data.Schema schema, Object value) {
    throw new UnsupportedOperationException(
        getClass().getName() + " only converts records read from Kafka");
  }

  private static Schema parseReaderSchema(Object configured) {
    if (configured == null) {
      return null;
    }
    if (!(configured instanceof String)) {
      throw new ConfigException(READER_SCHEMA_CONFIG, configured, "must be an Avro schema in JSON");
    }
    try {
      return new Schema.Parser().parse((String) configured);
    } catch (SchemaParseException e) {
      ConfigException error =
          new ConfigException(
              READER_SCHEMA_CONFIG, configured, "is not a valid Avro schema: " + e.getMessage());
      error.initCause(e);
      throw error;
    }
  }
}
