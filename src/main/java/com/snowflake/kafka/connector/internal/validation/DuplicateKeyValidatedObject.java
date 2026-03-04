/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/streaming/internal/serialization/DuplicateKeyValidatedObject.java
 *
 * Modifications:
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 *
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

/**
 * A wrapper for an Object that is going to be validated by {@link
 * DuplicateKeyValidatingSerializer}.
 */
public class DuplicateKeyValidatedObject {
  private final Object object;

  public DuplicateKeyValidatedObject(Object object) {
    this.object = object;
  }

  public Object getObject() {
    return object;
  }
}
