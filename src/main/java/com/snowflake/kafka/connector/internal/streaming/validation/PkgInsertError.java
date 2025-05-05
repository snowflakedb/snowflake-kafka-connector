package com.snowflake.kafka.connector.internal.streaming.validation;

import java.util.ArrayList;
import java.util.List;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;

/** Wraps the row content and exception when there is a failure */
class PkgInsertError {
  private final Object rowContent;
  private PkgSFException exception;

  // Used to map this error row with original row in the insertRows Iterable.
  // i.e the rowIndex can be index 9 in the list of 10 rows.
  // index is 0 based so as to match with incoming Iterable
  private long rowIndex;

  // List of extra column names in the input row compared with the table schema
  private List<String> extraColNames;

  // List of missing non-nullable column names in the input row compared with the table schema
  private List<String> missingNotNullColNames;

  // List of names of non-nullable column which have null value in the input row compared with the
  // table schema
  private List<String> nullValueForNotNullColNames;

  public PkgInsertError(Object row, long rowIndex) {
    this.rowContent = row;
    this.rowIndex = rowIndex;
  }

  /** Get the row content */
  public Object getRowContent() {
    return this.rowContent;
  }

  /** Get the exception message */
  public String getMessage() {
    return this.exception.getMessage();
  }

  /**
   * Set the insert exception
   *
   * @param exception exception encountered during the insert
   */
  public void setException(PkgSFException exception) {
    this.exception = exception;
  }

  /** Get the exception */
  public PkgSFException getException() {
    return this.exception;
  }

  /**
   * Set the row index
   *
   * @param rowIndex the corresponding row index in the original input row list
   */
  public void setRowIndex(long rowIndex) {
    this.rowIndex = rowIndex;
  }

  /**
   * Get the rowIndex. Please note, this index is 0 based so it can be used in fetching nth row from
   * the input. ({@link SnowflakeStreamingIngestChannel#insertRows(Iterable, String)})
   */
  public long getRowIndex() {
    return rowIndex;
  }

  /** Set the extra column names in the input row compared with the table schema */
  public void setExtraColNames(List<String> extraColNames) {
    this.extraColNames = extraColNames;
  }

  /**
   * Add an extra column name in the input row compared with the table schema
   *
   * @param extraColName the extra column name
   */
  public void addExtraColName(String extraColName) {
    if (extraColNames == null) {
      extraColNames = new ArrayList<>();
    }
    extraColNames.add(extraColName);
  }

  /** Get the list of extra column names in the input row compared with the table schema */
  public List<String> getExtraColNames() {
    return extraColNames;
  }

  /** Set the missing non-nullable column names in the input row compared with the table schema */
  public void setMissingNotNullColNames(List<String> missingNotNullColNames) {
    this.missingNotNullColNames = missingNotNullColNames;
  }

  /**
   * Add a missing non-nullable column name in the input row compared with the table schema
   *
   * @param missingNotNullColName the missing non-nullable column name
   */
  public void addMissingNotNullColName(String missingNotNullColName) {
    if (missingNotNullColNames == null) {
      missingNotNullColNames = new ArrayList<>();
    }
    missingNotNullColNames.add(missingNotNullColName);
  }

  /**
   * Get the list of missing non-nullable column names in the input row compared with the table
   * schema
   */
  public List<String> getMissingNotNullColNames() {
    return missingNotNullColNames;
  }

  /**
   * Set the list of names of non-nullable column which have null value in the input row compared
   * with the table schema
   */
  public void setNullValueForNotNullColNames(List<String> nullValueForNotNullColNames) {
    this.nullValueForNotNullColNames = nullValueForNotNullColNames;
  }

  /**
   * Add a name of non-nullable column which have null value in the input row compared with the
   * table schema
   *
   * @param nullValueForNotNullColName the name of non-nullable column which have null value
   */
  public void addNullValueForNotNullColName(String nullValueForNotNullColName) {
    if (nullValueForNotNullColNames == null) {
      nullValueForNotNullColNames = new ArrayList<>();
    }
    nullValueForNotNullColNames.add(nullValueForNotNullColName);
  }

  /**
   * Get the list of names of non-nullable column which have null value in the input row compared
   * with the table schema
   */
  public List<String> getNullValueForNotNullColNames() {
    return nullValueForNotNullColNames;
  }
}
