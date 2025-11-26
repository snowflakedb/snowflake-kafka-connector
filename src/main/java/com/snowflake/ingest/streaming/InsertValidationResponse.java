package com.snowflake.ingest.streaming;

import java.util.ArrayList;
import java.util.List;

/**
 * Response for an insert operation into a channel, which may contain a list of {@link InsertError}
 * objects if there is any failure during insertion.
 *
 * <p>Source: snowflake-ingest-sdk/java/net/snowflake/ingest/streaming/InsertValidationResponse.java
 */
public class InsertValidationResponse {
  // List of insertion errors, empty means no error
  private final List<InsertError> insertErrors = new ArrayList<>();

  /** Check whether there is any error during insertion */
  public boolean hasErrors() {
    return !insertErrors.isEmpty();
  }

  /** Get the list of insertion errors, the list is empty if no error */
  public List<InsertError> getInsertErrors() {
    return insertErrors;
  }

  /** Get the number of erroneous row count */
  public int getErrorRowCount() {
    return insertErrors.size();
  }

  /**
   * Add an insertion error to the error list
   *
   * @param error {@link InsertError} object which contains the row content and exception
   */
  public void addError(InsertError error) {
    insertErrors.add(error);
  }

  /** Wraps the row content and exception when there is a failure */
  public static class InsertError {
    private final Object rowContent;
    private SFException exception;
    private long rowIndex;
    private List<String> extraColNames;
    private List<String> missingNotNullColNames;
    private List<String> nullValueForNotNullColNames;

    public InsertError(Object row, long rowIndex) {
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
    public void setException(SFException exception) {
      this.exception = exception;
    }

    /** Get the exception */
    public SFException getException() {
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
     * Get the rowIndex. Please note, this index is 0 based so it can be used in fetching nth row
     * from the input.
     */
    public long getRowIndex() {
      return rowIndex;
    }

    /** Set the extra column names in the input row compared with the table schema */
    public void setExtraColNames(List<String> extraColNames) {
      this.extraColNames = extraColNames;
    }

    /** Add an extra column name */
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

    /** Add a missing non-nullable column name */
    public void addMissingNotNullColName(String missingNotNullColName) {
      if (missingNotNullColNames == null) {
        missingNotNullColNames = new ArrayList<>();
      }
      missingNotNullColNames.add(missingNotNullColName);
    }

    /** Get the list of missing non-nullable column names */
    public List<String> getMissingNotNullColNames() {
      return missingNotNullColNames;
    }

    /** Set the list of names of non-nullable column which have null value */
    public void setNullValueForNotNullColNames(List<String> nullValueForNotNullColNames) {
      this.nullValueForNotNullColNames = nullValueForNotNullColNames;
    }

    /** Add a name of non-nullable column which have null value */
    public void addNullValueForNotNullColName(String nullValueForNotNullColName) {
      if (nullValueForNotNullColNames == null) {
        nullValueForNotNullColNames = new ArrayList<>();
      }
      nullValueForNotNullColNames.add(nullValueForNotNullColName);
    }

    /** Get the list of names of non-nullable column which have null value */
    public List<String> getNullValueForNotNullColNames() {
      return nullValueForNotNullColNames;
    }
  }
}
