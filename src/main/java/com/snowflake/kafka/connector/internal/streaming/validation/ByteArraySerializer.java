package com.snowflake.kafka.connector.internal.streaming.validation;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class ByteArraySerializer extends JsonSerializer<byte[]> {
  @Override
  public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartArray();
    for (byte v : value) {
      gen.writeNumber(v);
    }
    gen.writeEndArray();
  }
}
