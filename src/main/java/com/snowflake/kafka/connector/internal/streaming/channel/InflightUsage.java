package com.snowflake.kafka.connector.internal.streaming.channel;

/** Rows and bytes accepted by one or more SDK channels and not yet acknowledged by Snowflake. */
public final class InflightUsage {

  public static final InflightUsage NONE = new InflightUsage(0L, 0L);

  private final long rows;
  private final long bytes;

  public InflightUsage(long rows, long bytes) {
    this.rows = rows;
    this.bytes = bytes;
  }

  public long getRows() {
    return rows;
  }

  public long getBytes() {
    return bytes;
  }

  public InflightUsage plus(InflightUsage other) {
    return new InflightUsage(rows + other.rows, bytes + other.bytes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InflightUsage)) {
      return false;
    }
    InflightUsage that = (InflightUsage) o;
    return rows == that.rows && bytes == that.bytes;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(rows) * 31 + Long.hashCode(bytes);
  }

  @Override
  public String toString() {
    return "InflightUsage{rows=" + rows + ", bytes=" + bytes + "}";
  }
}
