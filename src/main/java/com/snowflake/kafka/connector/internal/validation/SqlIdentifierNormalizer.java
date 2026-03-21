/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/streaming/internal/LiteralQuoteUtils.java
 *
 * Modifications:
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 * - Renamed from LiteralQuoteUtils to SqlIdentifierNormalizer
 * - Method names updated to reflect normalization semantics
 *
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */
package com.snowflake.kafka.connector.internal.validation;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * Normalizes SQL identifiers to their raw internal column names, matching server-side storage.
 *
 * <p>Rules:
 *
 * <ul>
 *   <li>Quoted identifier {@code "MyCol"} → strips quotes, preserves case → {@code MyCol}
 *   <li>Quoted with escaped quotes {@code "col""name"} → strips quotes, unescapes → {@code
 *       col"name}
 *   <li>Unquoted identifier {@code myCol} → uppercases → {@code MYCOL}
 * </ul>
 *
 * <p>Note: The methods in this class have to be kept in sync with the respective methods on server
 * side.
 */
public class SqlIdentifierNormalizer {

  /** Maximum number of normalized identifiers to store in cache */
  static final int NORMALIZED_IDENTIFIER_CACHE_MAX_SIZE = 30000;

  /** Cache storing normalized identifiers */
  private static final LoadingCache<String, String> normalizedIdentifierCache;

  static {
    normalizedIdentifierCache =
        Caffeine.newBuilder()
            .maximumSize(NORMALIZED_IDENTIFIER_CACHE_MAX_SIZE)
            .build(SqlIdentifierNormalizer::normalizeSqlIdentifierInternal);
  }

  /**
   * Normalize a SQL identifier to its raw internal column name. Uses a cache to avoid repeated
   * computation for the same identifier.
   *
   * @param sqlIdentifier the SQL identifier (may be quoted or unquoted)
   * @return the raw internal column name
   */
  public static String normalizeSqlIdentifier(String sqlIdentifier) {
    return normalizedIdentifierCache.get(sqlIdentifier);
  }

  /**
   * Normalize a SQL identifier to its raw internal column name.
   *
   * <p>Normalises the column name to how it is stored internally. This function needs to keep in
   * sync with server side normalisation.
   *
   * @param sqlIdentifier SQL identifier to normalize
   * @return raw internal column name
   */
  private static String normalizeSqlIdentifierInternal(String sqlIdentifier) {
    int length = sqlIdentifier.length();

    if (length == 0) {
      return sqlIdentifier;
    }

    // If this is an identifier that starts and ends with double quotes,
    // remove them - accounting for escaped double quotes.
    // Differs from the second condition in that this one allows repeated
    // double quotes
    if (sqlIdentifier.charAt(0) == '"'
        && (length >= 2
            && sqlIdentifier.charAt(length - 1) == '"'
            &&
            // Condition that the string contains no single double-quotes
            // but allows repeated double-quotes
            !sqlIdentifier.substring(1, length - 1).replace("\"\"", "").contains("\""))) {
      // Remove quotes and turn escaped double-quotes to single double-quotes
      return sqlIdentifier.substring(1, length - 1).replace("\"\"", "\"");
    }

    // If this is an identifier that starts and ends with double quotes,
    // remove them. Internal single double-quotes are not allowed.
    else if (sqlIdentifier.charAt(0) == '"'
        && (length >= 2
            && sqlIdentifier.charAt(length - 1) == '"'
            && !sqlIdentifier.substring(1, length - 1).contains("\""))) {
      // Remove the quotes
      return sqlIdentifier.substring(1, length - 1);
    }

    // unquoted string that can have escaped spaces
    else {
      // replace escaped spaces in unquoted name
      if (sqlIdentifier.contains("\\ ")) {
        sqlIdentifier = sqlIdentifier.replace("\\ ", " ");
      }
      return sqlIdentifier.toUpperCase();
    }
  }
}
