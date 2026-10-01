package com.snowflake.kafka.connector.records;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.snowflake.kafka.connector.internal.KCLogger;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * A {@code value.converter} that wraps Confluent's {@link AvroConverter} to additionally resolve
 * and expose the real Avro {@link Schema} for each record, keyed by topic. The wire format is
 * unchanged (magic byte + 4-byte schema id + Avro payload), so this reads the same schema id {@code
 * AvroConverter} already resolves internally, then delegates to a real {@code AvroConverter} for
 * the actual Struct conversion.
 */
public class SnowflakeAvroConverter implements Converter {

  private static final KCLogger LOGGER = new KCLogger(SnowflakeAvroConverter.class.getName());

  private static final byte CONFLUENT_MAGIC_BYTE = 0x0;
  private static final int MAGIC_BYTE_AND_SCHEMA_ID_BYTES = 5;
  private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 100;

  /**
   * Caps the number of distinct topics this converter tracks a schema for. Bounded (rather than a
   * plain map) so that long-running tasks with topic churn (e.g. a regex subscription) don't leak
   * memory one entry per topic ever seen.
   */
  private static final int MAX_TRACKED_TOPICS = 10_000;

  private final ConcurrentMap<String, Schema> latestAvroSchemaByTopic =
      Caffeine.newBuilder().maximumSize(MAX_TRACKED_TOPICS).<String, Schema>build().asMap();

  private SchemaRegistryClient schemaRegistryClient;
  private AvroConverter delegate;

  /** Used by Kafka Connect, which instantiates converters via a no-arg constructor. */
  public SnowflakeAvroConverter() {}

  /** Visible for tests, to inject mocks instead of talking to a real schema registry. */
  SnowflakeAvroConverter(SchemaRegistryClient schemaRegistryClient, AvroConverter delegate) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.delegate = delegate;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    List<String> registryUrls = parseSchemaRegistryUrls(configs);
    this.schemaRegistryClient =
        new CachedSchemaRegistryClient(registryUrls, DEFAULT_IDENTITY_MAP_CAPACITY, configs);
    // AvroConverter is given this same client instance rather than building its own, so its
    // internal re-resolution of the schema id we already looked up below is a cache hit, not a
    // second registry round trip.
    this.delegate = new AvroConverter(schemaRegistryClient);
    this.delegate.configure(configs, isKey);
  }

  @Override
  public byte[] fromConnectData(
      String topic, org.apache.kafka.connect.data.Schema schema, Object value) {
    return delegate.fromConnectData(topic, schema, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    Schema avroSchema = tryResolveAvroSchema(value);
    if (avroSchema != null) {
      latestAvroSchemaByTopic.put(topic, avroSchema);
    }
    return delegate.toConnectData(topic, value);
  }

  /**
   * Returns the real Avro {@link Schema} most recently resolved for {@code topic}, or {@code null}
   * if no record for that topic has been converted yet.
   *
   * <p>This holds a single schema per topic, not per record: if records for the same topic can
   * carry different schema versions (e.g. interleaved partitions on different versions), callers
   * must read this immediately after converting a given record, before converting another record
   * for the same topic, or they risk reading a schema that belongs to a different record.
   */
  public Schema getLatestAvroSchema(String topic) {
    return latestAvroSchemaByTopic.get(topic);
  }

  private Schema tryResolveAvroSchema(byte[] value) {
    if (value == null || value.length < MAGIC_BYTE_AND_SCHEMA_ID_BYTES) {
      return null;
    }
    if (value[0] != CONFLUENT_MAGIC_BYTE) {
      // Not Confluent wire-format Avro. Let the delegate converter raise the real error instead
      // of treating an arbitrary byte range as a schema id.
      return null;
    }
    int schemaId = ByteBuffer.wrap(value, 1, 4).getInt();
    try {
      return schemaRegistryClient.getById(schemaId);
    } catch (IOException | RestClientException e) {
      LOGGER.warn(
          "Failed to resolve Avro schema id {} from the schema registry: {}",
          schemaId,
          e.getMessage());
      return null;
    }
  }

  private static List<String> parseSchemaRegistryUrls(Map<String, ?> configs) {
    Object rawUrls = configs.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    if (rawUrls == null) {
      throw new ConfigException(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG + " must be set");
    }
    if (rawUrls instanceof List) {
      for (Object url : (List<?>) rawUrls) {
        if (!(url instanceof String)) {
          throw new ConfigException(
              AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
                  + " must only contain strings, but found: "
                  + url);
        }
      }
      @SuppressWarnings("unchecked")
      List<String> urls = (List<String>) rawUrls;
      return urls;
    }
    return Arrays.stream(rawUrls.toString().split(","))
        .map(String::trim)
        .collect(Collectors.toList());
  }
}
