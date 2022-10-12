package com.snowflake.kafka.connector.internal;

import com.google.common.base.MoreObjects;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Abstract class which holds buffered data per partition including its start offset, end offset,
 * size.
 *
 * <p>The getData() method returns the data specific to the implementation.
 *
 * <p>Buffer stores the converted records to Json format.
 *
 * <p>Long lived buffer would mean the data in partition would stay across two put APIs since the
 * buffer thresholds were not met.
 *
 * <p>Please check respective implementation class for more details.
 *
 * @param <T> Return type of {@link #getData()}
 */
public abstract class PartitionBuffer<T> {
  private int numOfRecords;
  private long bufferSizeBytes;
  private long firstOffset;
  private long lastOffset;

  /** @return Number of records in this buffer */
  public int getNumOfRecords() {
    return numOfRecords;
  }

  /** @return Buffer size in bytes */
  public long getBufferSizeBytes() {
    return bufferSizeBytes;
  }

  /** @return First offset number in this buffer */
  public long getFirstOffset() {
    return firstOffset;
  }

  /** @return Last offset number in this buffer */
  public long getLastOffset() {
    return lastOffset;
  }

  /** @param numOfRecords Updates number of records (Usually by 1) */
  public void setNumOfRecords(int numOfRecords) {
    this.numOfRecords = numOfRecords;
  }

  /** @param bufferSizeBytes Updates sum of size of records present in this buffer (Bytes) */
  public void setBufferSizeBytes(long bufferSizeBytes) {
    this.bufferSizeBytes = bufferSizeBytes;
  }

  /** @param firstOffset First offset no to set in this buffer */
  public void setFirstOffset(long firstOffset) {
    this.firstOffset = firstOffset;
  }

  /** @param lastOffset Last offset no to set in this buffer */
  public void setLastOffset(long lastOffset) {
    this.lastOffset = lastOffset;
  }

  /** @return true if buffer is empty */
  public boolean isEmpty() {
    return numOfRecords == 0;
  }

  /** Public constructor. */
  public PartitionBuffer() {
    numOfRecords = 0;
    bufferSizeBytes = 0;
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

  /**
   * Return the data that was buffered because buffer threshold might have been reached
   *
   * @return respective data type implemented by the class.
   */
  public abstract T getData();

  /**
   * TODO:SNOW-552576 Avoid extra memory in buffer.
   *
   * @return the sinkrecords corresponding to this buffer
   */
  public abstract List<SinkRecord> getSinkRecords();

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("numOfRecords", this.getNumOfRecords())
        .add("bufferSizeBytes", this.getBufferSizeBytes())
        .add("firstOffset", this.getFirstOffset())
        .add("lastOffset", this.getLastOffset())
        .toString();
  }
}
