/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/streaming/internal/serialization/DuplicateKeyValidatingSerializer.java
 *
 * Modifications:
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 *
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * A custom Jackson serializer that validates Objects by removing trailing nulls in keys for
 * duplication check. See SNOW-1772196 for more details.
 */
public class DuplicateKeyValidatingSerializer extends JsonSerializer<DuplicateKeyValidatedObject> {
  @Override
  public void serialize(
      DuplicateKeyValidatedObject value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    sanitizeAndWrite(value.getObject(), gen, serializers);
  }

  private void sanitizeAndWrite(Object object, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    if (object == null) {
      gen.writeNull();
      return;
    }
    if (object instanceof Map) {
      gen.writeStartObject();
      Map<?, ?> map = (Map<?, ?>) object;
      DuplicateDetector<String> duplicateDetector = new DuplicateDetector<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String key = entry.getKey().toString();
        String strippedKey = Utils.stripTrailingNulls(key);
        if (duplicateDetector.isDuplicate(strippedKey)) {
          throw new JsonGenerationException("Duplicate key in JSON object: " + key, gen);
        }
        gen.writeFieldName(key);
        sanitizeAndWrite(entry.getValue(), gen, serializers);
      }
      gen.writeEndObject();
    } else if (object instanceof List) {
      gen.writeStartArray();
      for (Object item : (List<?>) object) {
        sanitizeAndWrite(item, gen, serializers);
      }
      gen.writeEndArray();
    } else if (object.getClass().isArray()) {
      gen.writeStartArray();
      if (object.getClass().getComponentType().isPrimitive()) {
        final int length = Array.getLength(object);
        for (int i = 0; i < length; i++) {
          serializers.defaultSerializeValue(Array.get(object, i), gen);
        }
      } else {
        for (Object item : (Object[]) object) {
          sanitizeAndWrite(item, gen, serializers);
        }
      }
      gen.writeEndArray();
    } else {
      serializers.defaultSerializeValue(object, gen);
    }
  }
}
