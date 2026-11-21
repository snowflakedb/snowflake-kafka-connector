package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Reference implementation of the KC v3 {@code SnowflakeAvroConverter} decoding: Confluent wire
 * format, writer schema from the registry, optional reader schema, {@code GenericRecord.toString()}
 * parsed as JSON. Returns the {@code RECORD_CONTENT} document KC v3 wrote in Snowpipe mode.
 */
final class V3SnowpipeAvroReferenceDecoder {
  private final SchemaRegistryClient registry;
  private final ObjectMapper mapper = new ObjectMapper();

  V3SnowpipeAvroReferenceDecoder(SchemaRegistryClient registry) {
    this.registry = registry;
  }

  JsonNode decode(byte[] payload, Schema readerSchema) throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.get() != 0) {
      throw new IllegalArgumentException("not Confluent wire format");
    }
    int schemaId = buffer.getInt();
    Schema writerSchema = ((AvroSchema) registry.getSchemaById(schemaId)).rawSchema();
    byte[] data = new byte[buffer.remaining()];
    buffer.get(data);

    GenericData genericData = new GenericData();
    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
    GenericDatumReader<GenericRecord> reader =
        new GenericDatumReader<>(
            writerSchema, readerSchema == null ? writerSchema : readerSchema, genericData);
    Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);
    GenericRecord datum = reader.read(null, decoder);
    return mapper.readTree(datum.toString());
  }
}
