/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

/* * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved. * * This file is copied from the Snowflake Ingest SDK v1 (utils/DuplicateDetector.java) * to support client-side validation in Kafka Connector v4. */package com.snowflake.kafka.connector.internal.validation;

import java.util.HashSet;
import java.util.Set;

/**
 * A utility class that detects duplicate objects. Optimized for Json objects with a small number of
 * keys.
 */
public class DuplicateDetector<T> {
  private T firstKey;
  private T secondKey;
  private Set<T> keys;

  public boolean isDuplicate(T key) {
    if (firstKey == null) {
      firstKey = key;
      return false;
    }
    if (firstKey.equals(key)) {
      return true;
    }
    if (secondKey == null) {
      secondKey = key;
      return false;
    }
    if (secondKey.equals(key)) {
      return true;
    }

    if (keys == null) {
      keys = new HashSet<>();
    }
    return !keys.add(key);
  }
}
