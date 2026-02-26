/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

/* * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved. * * This file is copied from the Snowflake Ingest SDK v1 (streaming/internal/serialization/ZonedDateTimeSerializer.java) * to support client-side validation in Kafka Connector v4. */package com.snowflake.kafka.connector.internal.validation;

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
