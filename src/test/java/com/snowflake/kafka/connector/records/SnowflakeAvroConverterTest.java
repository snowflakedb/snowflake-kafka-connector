package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

public class SnowflakeAvroConverterTest {

  private static final String TOPIC = "test-topic";

  private static byte[] wireBytesFor(int schemaId, byte... payload) {
    ByteBuffer buffer = ByteBuffer.allocate(5 + payload.length);
    buffer.put((byte) 0);
    buffer.putInt(schemaId);
    buffer.put(payload);
    return buffer.array();
  }

  private static org.apache.kafka.connect.data.Schema connectStructSchema() {
    return org.apache.kafka.connect.data.SchemaBuilder.struct()
        .name("MyRecord")
        .field("a", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();
  }

  @Test
  public void toConnectData_resolvesRealAvroSchema_attachesItToTheConnectSchemaAndStruct()
      throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    Schema avroSchema =
        SchemaBuilder.record("MyRecord")
            .fields()
            .name("a")
            .type()
            .stringType()
            .noDefault()
            .endRecord();
    byte[] wireBytes = wireBytesFor(42, (byte) 1, (byte) 2);
    org.apache.kafka.connect.data.Schema connectSchema = connectStructSchema();
    Struct struct = new Struct(connectSchema).put("a", "hello");
    SchemaAndValue expected = new SchemaAndValue(connectSchema, struct);

    when(registryClient.getById(42)).thenReturn(avroSchema);
    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, wireBytes);

    Optional<Schema> extracted = SnowflakeAvroConverter.extractAvroSchema(actual.schema());
    assertTrue(extracted.isPresent());
    assertEquals(avroSchema, extracted.get());
    // The attached Struct's own schema must be the same object as the record's valueSchema, so
    // struct.schema() and SinkRecord.valueSchema() agree.
    Struct actualStruct = (Struct) actual.value();
    assertSame(actual.schema(), actualStruct.schema());
    assertEquals("hello", actualStruct.get("a"));
    verify(delegate).toConnectData(TOPIC, wireBytes);
  }

  @Test
  public void toConnectData_noConnectSchema_resolvesButHasNothingToAttachTo() throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    Schema avroSchema =
        SchemaBuilder.record("MyRecord")
            .fields()
            .name("a")
            .type()
            .stringType()
            .noDefault()
            .endRecord();
    byte[] wireBytes = wireBytesFor(42, (byte) 1, (byte) 2);
    SchemaAndValue expected = new SchemaAndValue(null, "converted");

    when(registryClient.getById(42)).thenReturn(avroSchema);
    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, wireBytes);

    assertSame(expected, actual);
    assertFalse(SnowflakeAvroConverter.extractAvroSchema(actual.schema()).isPresent());
    verify(delegate).toConnectData(TOPIC, wireBytes);
  }

  @Test
  public void toConnectData_registryLookupFails_stillDelegatesAndAttachesNothing()
      throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    byte[] wireBytes = wireBytesFor(7);
    org.apache.kafka.connect.data.Schema connectSchema = connectStructSchema();
    SchemaAndValue expected =
        new SchemaAndValue(connectSchema, new Struct(connectSchema).put("a", "hello"));

    when(registryClient.getById(7)).thenThrow(new java.io.IOException("boom"));
    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, wireBytes);

    assertSame(expected, actual);
    assertFalse(SnowflakeAvroConverter.extractAvroSchema(actual.schema()).isPresent());
    verify(delegate).toConnectData(TOPIC, wireBytes);
  }

  @Test
  public void toConnectData_valueTooShortForHeader_skipsLookupAndDelegates() throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    byte[] tooShort = new byte[] {0, 1, 2};
    SchemaAndValue expected = new SchemaAndValue(null, "converted");

    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, tooShort);

    assertSame(expected, actual);
    verify(registryClient, never()).getById(any(Integer.class));
    verify(delegate).toConnectData(TOPIC, tooShort);
  }

  @Test
  public void toConnectData_wrongMagicByte_skipsLookupAndDelegates() throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    byte[] wrongMagicByte = wireBytesFor(42, (byte) 1, (byte) 2);
    wrongMagicByte[0] = (byte) 5;
    SchemaAndValue expected = new SchemaAndValue(null, "converted");

    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, wrongMagicByte);

    assertSame(expected, actual);
    verify(registryClient, never()).getById(any(Integer.class));
    verify(delegate).toConnectData(TOPIC, wrongMagicByte);
  }

  @Test
  public void extractAvroSchema_nullSchema_returnsEmpty() {
    assertFalse(SnowflakeAvroConverter.extractAvroSchema(null).isPresent());
  }

  @Test
  public void configure_schemaRegistryUrlListContainsNonString_throwsConfigException() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        Arrays.asList("http://good:8081", 42));

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter();

    assertThrows(ConfigException.class, () -> converter.configure(configs, false));
  }

  @Test
  public void configure_schemaRegistryUrlMissing_throwsConfigException() {
    SnowflakeAvroConverter converter = new SnowflakeAvroConverter();

    assertThrows(ConfigException.class, () -> converter.configure(new HashMap<>(), false));
  }

  @Test
  public void fromConnectData_delegatesDirectly() {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    byte[] expected = new byte[] {9, 9, 9};

    when(delegate.fromConnectData(eq(TOPIC), any(), any())).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    byte[] actual = converter.fromConnectData(TOPIC, null, "value");

    assertSame(expected, actual);
    verify(delegate).fromConnectData(TOPIC, null, "value");
  }
}
