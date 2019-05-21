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

import java.util.Comparator;

/**
 * SortFileNamesByFirstOffset is a comparable for file names by first offset
 */

class SortFileNamesByFirstOffset implements Comparator<String>
{
  public int compare(String fileA, String fileB)
  {

    return Long.compare(
      Long.valueOf(Utils.fileNameToStartOffset(fileA)),
      Long.valueOf(Utils.fileNameToStartOffset(fileB)));

  }

//    /* may not be safe to override equals method */
//    public boolean equals(String fileA, String fileB)
//    {
//        return (fileA == fileB);
//    }
}
