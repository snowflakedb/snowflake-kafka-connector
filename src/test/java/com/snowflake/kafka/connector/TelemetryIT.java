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

import org.junit.Ignore;

import java.util.LinkedList;
import java.util.List;

public class TelemetryIT
{
  /**
   * manual test only
   */
  @Ignore
  public void TelemetryTest() throws Exception
  {
    SnowflakeJDBCWrapper jdbc = new SnowflakeJDBCWrapper(TestUtils.getConf());

    SnowflakeTelemetry telemetry = jdbc.getTelemetry();

    String appName = "Kafka_test_app_name";

    telemetry.reportKafkaStart(Utils.currentTime(), 1, 1, 1, appName);

    telemetry.reportKafkaStop(Utils.currentTime(), appName);

    telemetry.reportKafkaUsage(Utils.currentTime(), Utils.currentTime(),
      123, 456, appName);

    telemetry.reportKafkaCreatePipe("test pipe", "test stage",
      "test table", appName);

    telemetry.reportKafkaCreateStage("test stage", appName);

    telemetry.reportKafkaCreateTable("test table", appName);

    List<String> files = new LinkedList<>();

    files.add("file1");

    files.add("file2");

    telemetry.reportKafkaFileFailed("test table", "test stage", files, appName);
  }
}
