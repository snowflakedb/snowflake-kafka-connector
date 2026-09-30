package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaAndValue;
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

  @Test
  public void toConnectData_resolvesAndStashesRealAvroSchema_thenDelegates() throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    Schema avroSchema = SchemaBuilder.record("MyRecord").fields().name("a").type().stringType().noDefault().endRecord();
    byte[] wireBytes = wireBytesFor(42, (byte) 1, (byte) 2);
    SchemaAndValue expected = new SchemaAndValue(null, "converted");

    when(registryClient.getById(42)).thenReturn(avroSchema);
    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, wireBytes);

    assertSame(expected, actual);
    assertSame(avroSchema, converter.getLatestAvroSchema(TOPIC));
    verify(delegate).toConnectData(TOPIC, wireBytes);
  }

  @Test
  public void toConnectData_registryLookupFails_stillDelegatesAndLeavesSchemaUnset() throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    byte[] wireBytes = wireBytesFor(7);
    SchemaAndValue expected = new SchemaAndValue(null, "converted");

    when(registryClient.getById(7)).thenThrow(new java.io.IOException("boom"));
    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, wireBytes);

    assertSame(expected, actual);
    assertNull(converter.getLatestAvroSchema(TOPIC));
    verify(delegate).toConnectData(TOPIC, wireBytes);
  }

  @Test
  public void toConnectData_valueTooShortForHeader_skipsLookupAndDelegates() throws Exception {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    byte[] tooShort = new byte[]{0, 1, 2};
    SchemaAndValue expected = new SchemaAndValue(null, "converted");

    when(delegate.toConnectData(eq(TOPIC), any(byte[].class))).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    SchemaAndValue actual = converter.toConnectData(TOPIC, tooShort);

    assertSame(expected, actual);
    assertNull(converter.getLatestAvroSchema(TOPIC));
    verify(registryClient, never()).getById(any(Integer.class));
    verify(delegate).toConnectData(TOPIC, tooShort);
  }

  @Test
  public void fromConnectData_delegatesDirectly() {
    SchemaRegistryClient registryClient = mock(SchemaRegistryClient.class);
    AvroConverter delegate = mock(AvroConverter.class);
    byte[] expected = new byte[]{9, 9, 9};

    when(delegate.fromConnectData(eq(TOPIC), any(), any())).thenReturn(expected);

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter(registryClient, delegate);
    byte[] actual = converter.fromConnectData(TOPIC, null, "value");

    assertSame(expected, actual);
    verify(delegate).fromConnectData(TOPIC, null, "value");
  }
}
