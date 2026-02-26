/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

/* * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved. * * This file is copied from the Snowflake Ingest SDK v1 (streaming/internal/serialization/DuplicateKeyValidatedObject.java) * to support client-side validation in Kafka Connector v4. */package com.snowflake.kafka.connector.internal.validation;

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
