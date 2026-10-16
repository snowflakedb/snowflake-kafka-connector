package com.snowflake.kafka.connector.records;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * A {@code value.converter} that wraps Confluent's {@link AvroConverter} to additionally resolve
 * the real Avro {@link Schema} for each record and attach it to the Connect {@code Schema} this
 * converter returns, as a {@link org.apache.kafka.connect.data.Schema#parameters()} entry under
 * {@link #AVRO_SCHEMA_PARAMETER_KEY}. The wire format is unchanged (magic byte + 4-byte schema id +
 * Avro payload), so this reads the same schema id {@code AvroConverter} already resolves
 * internally, then delegates to a real {@code AvroConverter} for the actual Struct conversion.
 *
 * <p>The schema rides on the record itself (via {@code SinkRecord.valueSchema()}, recoverable with
 * {@link #extractAvroSchema}) rather than a converter-side cache keyed by topic, so it stays
 * correct per record even when interleaved records on the same topic carry different schema
 * versions, and doesn't require whoever reads it (e.g. the sink task) to hold a reference to this
 * converter instance.
 */
public class SnowflakeAvroConverter implements Converter {

  private static final byte CONFLUENT_MAGIC_BYTE = 0x0;
  private static final int MAGIC_BYTE_AND_SCHEMA_ID_BYTES = 5;
  private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 100;

  /**
   * Schema parameter key under which the resolved Avro {@link Schema}, serialized via {@link
   * Schema#toString()}, is stashed on the Connect {@code Schema} returned from {@link
   * #toConnectData}. Recover it with {@link #extractAvroSchema}.
   */
  public static final String AVRO_SCHEMA_PARAMETER_KEY =
      "com.snowflake.kafka.connector.avro.schema";

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
    // Built the same way AvroConverter builds its own client, rather than via a hardcoded
    // `new CachedSchemaRegistryClient(...)`, so a `mock://` scoped test URL (or any other
    // non-default SchemaRegistryClient the factory knows how to produce) resolves the same way
    // here as it does for AvroConverter itself.
    this.schemaRegistryClient =
        SchemaRegistryClientFactory.newClient(
            registryUrls,
            DEFAULT_IDENTITY_MAP_CAPACITY,
            Collections.singletonList(new AvroSchemaProvider()),
            configs,
            null);
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
    SchemaAndValue converted = delegate.toConnectData(topic, value);
    return avroSchema == null ? converted : attachAvroSchema(converted, avroSchema);
  }

  /**
   * Recovers the Avro {@link Schema} that {@link #toConnectData} attached to {@code connectSchema},
   * if any. Returns {@link Optional#empty} if {@code connectSchema} is {@code null}, carries no
   * such parameter (e.g. it wasn't produced by this converter, or schema resolution failed for that
   * record), or the attached text isn't a parsable Avro schema.
   */
  public static Optional<Schema> extractAvroSchema(
      org.apache.kafka.connect.data.Schema connectSchema) {
    if (connectSchema == null || connectSchema.parameters() == null) {
      return Optional.empty();
    }
    String avroSchemaJson = connectSchema.parameters().get(AVRO_SCHEMA_PARAMETER_KEY);
    return avroSchemaJson == null
        ? Optional.empty()
        : Optional.of(new Schema.Parser().parse(avroSchemaJson));
  }

  /**
   * Rebuilds {@code converted}'s top-level Connect schema with {@code avroSchema} attached as a
   * parameter, and -- if the converted value is a {@link Struct} -- rebuilds the Struct against
   * that new schema object too, so {@code struct.schema()} and {@code SinkRecord.valueSchema()}
   * agree rather than silently diverging.
   */
  private static SchemaAndValue attachAvroSchema(SchemaAndValue converted, Schema avroSchema) {
    org.apache.kafka.connect.data.Schema connectSchema = converted.schema();
    if (connectSchema == null
        || connectSchema.type() != org.apache.kafka.connect.data.Schema.Type.STRUCT) {
      // Only a top-level Avro record converts to a STRUCT; anything else (schemas disabled, or a
      // non-record top-level Avro schema) has no struct-level parameters to attach to.
      return converted;
    }
    org.apache.kafka.connect.data.Schema annotatedSchema =
        withAvroSchemaParameter(connectSchema, avroSchema);
    Object value = converted.value();
    if (value instanceof Struct) {
      value = copyStructOntoSchema((Struct) value, annotatedSchema);
    }
    return new SchemaAndValue(annotatedSchema, value);
  }

  private static org.apache.kafka.connect.data.Schema withAvroSchemaParameter(
      org.apache.kafka.connect.data.Schema original, Schema avroSchema) {
    SchemaBuilder builder = SchemaBuilder.struct();
    if (original.name() != null) {
      builder.name(original.name());
    }
    if (original.version() != null) {
      builder.version(original.version());
    }
    if (original.doc() != null) {
      builder.doc(original.doc());
    }
    if (original.parameters() != null) {
      original.parameters().forEach(builder::parameter);
    }
    builder.parameter(AVRO_SCHEMA_PARAMETER_KEY, avroSchema.toString());
    for (Field field : original.fields()) {
      builder.field(field.name(), field.schema());
    }
    return original.isOptional() ? builder.optional().build() : builder.build();
  }

  private static Struct copyStructOntoSchema(
      Struct original, org.apache.kafka.connect.data.Schema newSchema) {
    Struct copy = new Struct(newSchema);
    for (Field field : newSchema.fields()) {
      copy.put(field.name(), original.get(field.name()));
    }
    return copy;
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
      // Fail loudly rather than silently converting the record without the Avro schema attached
      // -- a registry lookup failure here means delegate.toConnectData's own resolution of the
      // same id is likely to fail too, so swallowing this would just defer to a less informative
      // error (or none at all, if the delegate's resolution happens to succeed on a retry).
      throw new DataException(
          "Failed to resolve Avro schema id " + schemaId + " from the schema registry", e);
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
