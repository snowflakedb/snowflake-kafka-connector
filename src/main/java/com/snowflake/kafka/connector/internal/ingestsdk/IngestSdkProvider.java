/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
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

package com.snowflake.kafka.connector.internal.ingestsdk;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is a singleton provider that provides global access to the static managers of the ingest sdk
 */
public class IngestSdkProvider {
  // manages the streaming ingest sdk client
  private static StreamingClientManager streamingClientManager = new StreamingClientManager();

  // private constructor for singletons
  private IngestSdkProvider() {}

  public static StreamingClientManager getStreamingClientManager() {
    assert streamingClientManager != null;
    return streamingClientManager;
  }

  // TESTING ONLY
  @VisibleForTesting
  public static void setStreamingClientManager(StreamingClientManager manager) {
    streamingClientManager = manager;
  }
}
