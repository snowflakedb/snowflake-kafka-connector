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

/**
 * SnowflakeFileMetadata keeps track of the life cycle of a file
 */
class SnowflakeFileMetadata
{
  String fileName;
  long startOffset;
  long endOffset;
  long timeIngested;  // timestamp when file was sent to snowpipe

  SnowflakeFileMetadata(String fileName)
  {
    this.fileName = fileName;
    this.startOffset = Utils.fileNameToStartOffset(fileName);
    this.endOffset = Utils.fileNameToEndOffset(fileName);
    this.timeIngested = Utils.fileNameToTimeIngested(fileName);
  }

    /*
    long timeVerified;  // timestamp when the file status was checked as
    'finalized'
    Utils.IngestedFileStatus fileStatus;    // 'finalized' status of the file

    SnowflakeFileMetadata(String fileName, Utils.IngestedFileStatus
    fileStatus, long timeVerfied)
    {
        this.fileName = fileName;
        this.startOffset = Utils.fileNameToStartOffset(fileName);
        this.endOffset = Utils.fileNameToEndOffset(fileName);
        this.fileStatus = fileStatus;
        this.timeIngested = Utils.fileNameToTimeIngested(fileName);
        this.timeVerified = timeVerfied;
    }

    void setFileStatus(Utils.IngestedFileStatus fileStatus)
    {
        this.fileStatus = fileStatus;
    }

    void setTimeVerified(long time)
    {
        this.timeVerified = time;
    }
    */
}