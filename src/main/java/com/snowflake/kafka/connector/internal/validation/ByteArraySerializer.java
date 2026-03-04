/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/streaming/internal/serialization/ByteArraySerializer.java
 *
 * Modifications:
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 *
 * Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

/**
 * Serialize Java byte arrays as JSON arrays of numbers instead of the default Jackson
 * base64-encoding.
 */
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
