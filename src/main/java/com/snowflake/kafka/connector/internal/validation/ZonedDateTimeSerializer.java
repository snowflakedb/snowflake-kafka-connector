/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/streaming/internal/serialization/ZonedDateTimeSerializer.java
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
import java.time.ZonedDateTime;

/** Snowflake does not support parsing zones, so serialize it in offset instead */
public class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime> {
  @Override
  public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeString(value.toOffsetDateTime().toString());
  }
}
