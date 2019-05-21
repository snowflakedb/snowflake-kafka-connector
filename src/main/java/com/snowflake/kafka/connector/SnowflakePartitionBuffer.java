/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * SnowflakePartitionBuffer caches all the sink records received so far
 * from kafka connect for a partition, until they are ready to be written to
 * a file
 * and ingested through snowpipe.
 * It also tracks the start offset, end offset, and estimated file size.
 */
class SnowflakePartitionBuffer
{
  private String buffer;                  // all records serialized as
    // String, with or without metadata
  private int recordCount;                // number of records current in the
    // buffer
  private int bufferSize;                 // cumulative size of records in
    // the buffer
  private long firstOffset;               // offset of the first record in
    // the buffer
  private long firstOffsetTime;           // buffer start time

  // offset of the latest record that was "processed" on this partition
  // in previous incarnation or current incarnation of the connector
  // This is set during connector recovery and updated as we process new records
  private long latestOffset;

  // offset of the latest record that was "committed" on this partition
  // in previous incarnation or current incarnation of the connector
  // This is set by preCommit
  private long committedOffset;


  SnowflakePartitionBuffer()
  {
    buffer = "";
    recordCount = 0;
    bufferSize = 0;
    firstOffset = -1;
    firstOffsetTime = 0;
    latestOffset = -1;
    committedOffset = -1;
  }

  void bufferRecord(SinkRecord record, String recordAsString)
  {
    // initialize if this is the first record entering buffer
    if (bufferSize == 0)
    {
      firstOffset = record.kafkaOffset();
      firstOffsetTime = System.currentTimeMillis();
    }

    buffer += recordAsString;
    recordCount++;
    bufferSize += recordAsString.length();
    latestOffset = record.kafkaOffset();
  }

  void dropRecords()
  {
    buffer = "";
    recordCount = 0;
    bufferSize = 0;
    firstOffset = -1;
    firstOffsetTime = 0;

    // NOTE: don't touch latestOffset and committedOffset as those have a
      // forever life cycle
  }

  String bufferAsString()
  {
    return buffer;
  }

  int recordCount()
  {
    return recordCount;
  }

  long bufferSize()
  {
    return bufferSize;
  }

  long firstOffset()
  {
    return firstOffset;
  }

  long getFirstOffsetTime()
  {
    return firstOffsetTime;
  }

  long latestOffset()
  {
    return this.latestOffset;
  }

  long committedOffset()
  {
    return this.committedOffset;
  }

  // this method is called during connector recovery and
  // when we see a malformed json record
  void setLatestOffset(long offset)
  {
    this.latestOffset = offset;
  }

  // this method is only called from preCommit
  void setCommittedOffset(long offset)
  {
    this.committedOffset = offset;
  }
}