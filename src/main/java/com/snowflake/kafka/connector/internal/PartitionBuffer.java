package com.snowflake.kafka.connector.internal;

import org.apache.kafka.connect.sink.SinkRecord;

public abstract class PartitionBuffer<T> {
  private int numOfRecord;
  private int bufferSize;
  private long firstOffset;
  private long lastOffset;

  public int getNumOfRecord() {
    return numOfRecord;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public long getFirstOffset() {
    return firstOffset;
  }

  public long getLastOffset() {
    return lastOffset;
  }

  public void setNumOfRecord(int numOfRecord) {
    this.numOfRecord = numOfRecord;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public void setFirstOffset(long firstOffset) {
    this.firstOffset = firstOffset;
  }

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
