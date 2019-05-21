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

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
  .ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
  .ObjectNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

public class JsonRecordServiceTest
{
  private SinkRecord record;

  private SinkRecord brokenRecord;

  private ObjectNode expectedContent;

  private ObjectNode expectedMeta;

  private ObjectMapper mapper = new ObjectMapper();

  @Before
  public void beforeAll()
  {
    record =
      new SinkRecord(
        "test_topic",
        123,
        Schema.STRING_SCHEMA,
        "test_key",
        Schema.STRING_SCHEMA,
        "{\"data\":\"test content\"}",
        45678
      );

    expectedContent = mapper.createObjectNode().put("data", "test content");

    expectedMeta = mapper.createObjectNode();

    expectedMeta.put("offset", 45678);

    expectedMeta.put("topic", "test_topic");

    expectedMeta.put("partition", 123);

    brokenRecord =
      new SinkRecord(
        "test_topic",
        123,
        Schema.STRING_SCHEMA,
        "test_key",
        Schema.STRING_SCHEMA,
        "{\"data\":test content}",
        45678
      );
  }

  @Test
  public void testDefaultConstructor()
  {
    RecordService service = new JsonRecordService();

    ObjectNode result = mapper.createObjectNode();

    result.set("content", expectedContent);

    result.set("meta", expectedMeta);

    assert service.processRecord(record).equals(result.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDefaultConstructorWithBrokenData()
  {
    RecordService service = new JsonRecordService();

    service.processRecord(brokenRecord);
  }

  @Test
  public void testDefaultConstructorWithSpecialChar()
  {
    RecordService service = new JsonRecordService();

    SinkRecord record =
      new SinkRecord(
        "test_topic",
        123,
        Schema.STRING_SCHEMA,
        "test_key",
        Schema.STRING_SCHEMA,
        "{\"int\":-300723322,\"long\":-8470814646386700253," +
          "\"double\":0.41142795289020617," +
          "\"text\":\"ꃤ⎀솻㐟뇕嶽Ὡꑿ폆ꫨ輂ꤗ㝸征厨辠瑌짎餷嗐\",\"boolean\":false}",
        45678
      );

    service.processRecord(record);
  }

  @Test
  public void testMultipleConfig()
  {
    //no offset
    RecordService service = new JsonRecordService(false, true, true);

    String result = service.processRecord(record);

    ObjectNode noOffset = mapper.createObjectNode();

    noOffset.set("content", expectedContent);

    ObjectNode tmpMeta = expectedMeta.deepCopy();

    tmpMeta.remove("offset");

    noOffset.set("meta", tmpMeta);

    String expect = noOffset.toString();

    assert result.equals(expect);

    //no partition number
    service = new JsonRecordService(true, false, true);

    result = service.processRecord(record);

    ObjectNode noPartition = mapper.createObjectNode();

    noPartition.set("content", expectedContent);

    tmpMeta = expectedMeta.deepCopy();

    tmpMeta.remove("partition");

    noPartition.set("meta", tmpMeta);

    expect = noPartition.toString();

    assert result.equals(expect);

    //no topic
    service = new JsonRecordService(true, true, false);

    result = service.processRecord(record);

    ObjectNode noTopic = mapper.createObjectNode();

    noTopic.set("content", expectedContent);

    tmpMeta = expectedMeta.deepCopy();

    tmpMeta.remove("topic");

    noTopic.set("meta", tmpMeta);

    expect = noTopic.toString();

    assert result.equals(expect);

  }
}
