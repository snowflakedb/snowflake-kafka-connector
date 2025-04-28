/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */
package com.snowflake.kafka.connector.internal.streaming.validation;

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
