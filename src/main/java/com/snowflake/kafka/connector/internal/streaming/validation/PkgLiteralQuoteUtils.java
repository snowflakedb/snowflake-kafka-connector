/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */
package com.snowflake.kafka.connector.internal.streaming.validation;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * Util class to normalise literals to match server side metadata.
 *
 * <p>Note: The methods in this class have to be kept in sync with the respective methods on server
 * side.
 */
class PkgLiteralQuoteUtils {

  /** Maximum number of unquoted column names to store in cache */
  static final int UNQUOTED_COLUMN_NAME_CACHE_MAX_SIZE = 30000;

  /** Cache storing unquoted column names */
  private static final LoadingCache<String, String> unquotedColumnNamesCache;

  static {
    unquotedColumnNamesCache =
        Caffeine.newBuilder()
            .maximumSize(UNQUOTED_COLUMN_NAME_CACHE_MAX_SIZE)
            .build(PkgLiteralQuoteUtils::unquoteColumnNameInternal);
  }

  /**
   * Unquote column name expected to be used from the outside. It decides is unquoting would be
   * expensive. If not, it unquotes directly, otherwise it return a value from a loading cache.
   */
  static String unquoteColumnName(String columnName) {
    return unquotedColumnNamesCache.get(columnName);
  }

  /**
   * Unquote SQL literal.
   *
   * <p>Normalises the column name to how it is stored internally. This function needs to keep in
   * sync with server side normalisation. The reason, we do it here, is to store the normalised
   * column name in BDEC file metadata.
   *
   * @param columnName column name literal to unquote
   * @return unquoted literal
   */
  static String unquoteColumnNameInternal(String columnName) {
    int length = columnName.length();

    if (length == 0) {
      return columnName;
    }

    // If this is an identifier that starts and ends with double quotes,
    // remove them - accounting for escaped double quotes.
    // Differs from the second condition in that this one allows repeated
    // double quotes
    if (columnName.charAt(0) == '"'
        && (length >= 2
            && columnName.charAt(length - 1) == '"'
            &&
            // Condition that the string contains no single double-quotes
            // but allows repeated double-quotes
            !columnName.substring(1, length - 1).replace("\"\"", "").contains("\""))) {
      // Remove quotes and turn escaped double-quotes to single double-quotes
      return columnName.substring(1, length - 1).replace("\"\"", "\"");
    }

    // If this is an identifier that starts and ends with double quotes,
    // remove them. Internal single double-quotes are not allowed.
    else if (columnName.charAt(0) == '"'
        && (length >= 2
            && columnName.charAt(length - 1) == '"'
            && !columnName.substring(1, length - 1).contains("\""))) {
      // Remove the quotes
      return columnName.substring(1, length - 1);
    }

    // unquoted string that can have escaped spaces
    else {
      // replace escaped spaces in unquoted name
      if (columnName.contains("\\ ")) {
        columnName = columnName.replace("\\ ", " ");
      }
      return columnName.toUpperCase();
    }
  }
}
