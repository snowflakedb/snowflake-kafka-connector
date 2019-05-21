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
 * AVRO Record Service functions
 */
public class AvroRecordService implements RecordService
{

  /**
   * process AVRO records
   *
   * @param includeOffset          if true, the output will contain offset
   * @param includePartitionNumber if true, the output will contain
   *                               partition number
   * @param includeTopic           if true, the output will contain topic name
   */
  public AvroRecordService(boolean includeOffset, boolean
    includePartitionNumber, boolean includeTopic)
  {
  }

  /**
   * create a AvroRecordService instance
   */
  public AvroRecordService()
  {
    this(true, true, true);
  }

  /**
   * process given SinkRecord
   *
   * @param record SinkRecord
   * @return a AVRO string, already to output
   * @throws IllegalArgumentException if the given record is broken
   */
  @Override
  public String processRecord(final SinkRecord record) throws
    IllegalArgumentException
  {
    // We don't validate AVRO records, unlike JSON
    // Simply return string representation of the record content
    return record.value().toString();
  }
}
