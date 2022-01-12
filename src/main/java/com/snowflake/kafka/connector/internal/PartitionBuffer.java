package com.snowflake.kafka.connector.internal;

import java.util.Collection;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Abstract class which holds buffered data per partition including its start offset, end offset,
 * size.
 *
 * <p>The getData() method returns the data specific to the implementation.
 *
 * <p>Buffer stores the converted records to Json format which can be temporary or long lived.
 *
 * <p>Temporary would mean the partition buffer is emptied when all records from {@link
 * com.snowflake.kafka.connector.SnowflakeSinkTask#put(Collection)} are processed and respective API
 * is called.
 *
 * <p>Long lived would mean the data in partition would stay across two put APIs since the buffer
 * thresholds were not met.
 *
 * <p>Please check respective implementation class for more details.
 *
 * @param <T> Return type of {@link #getData()}
 */
public abstract class PartitionBuffer<T> {
  private int numOfRecord;
  private int bufferSize;
  private long firstOffset;
  private long lastOffset;

  /** Get number of records in this buffer */
  public int getNumOfRecord() {
    return numOfRecord;
  }

  /* Get buffer size */
  public int getBufferSize() {
    return bufferSize;
  }

  /* Get first offset number in this buffer */
  public long getFirstOffset() {
    return firstOffset;
  }

  /* Get last offset number in this buffer */
  public long getLastOffset() {
    return lastOffset;
  }

  /* Updates number of records (Usually by 1) */
  public void setNumOfRecord(int numOfRecord) {
    this.numOfRecord = numOfRecord;
  }

  /* Updates sum of size of records present in this buffer */
  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  /* Sets first offset no */
  public void setFirstOffset(long firstOffset) {
    this.firstOffset = firstOffset;
  }

  /* Sets last offset no */
  public void setLastOffset(long lastOffset) {
    this.lastOffset = lastOffset;
  }

  public boolean isEmpty() {
    return numOfRecord == 0;
  }

  public PartitionBuffer() {
    numOfRecord = 0;
    bufferSize = 0;
    firstOffset = -1;
    lastOffset = -1;
  }

  /**
   * Inserts the row into Buffer.
   *
   * @param record the record which kafka sends to KC. We also convert the record to a format which
   *     is understood by Snowflake Table
   */
  public abstract void insert(SinkRecord record);

  // return the data that was buffered because buffer threshold might have been reached
  public abstract T getData();
}
