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
package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.records.RecordService.ISO_DATE_TIME_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.jupiter.api.Test;

public class ConverterTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final ThreadLocal<GregorianCalendar> CALENDAR_THREAD_SAFE =
      ThreadLocal.withInitial(
          () -> {
            GregorianCalendar gregorianCalendar =
                new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
            gregorianCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
            gregorianCalendar.add(Calendar.DATE, 10000);
            return gregorianCalendar;
          });

  @Test
  public void testConnectJsonConverter_MapInt64() throws JsonProcessingException {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, Object> jsonMap = new HashMap<>();
    // Value will map to int64.
    jsonMap.put("test", Integer.MAX_VALUE);
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("test", mapper.writeValueAsBytes(jsonMap));
    JsonNode result =
        RecordService.convertToJson(schemaAndValue.schema(), schemaAndValue.value(), false);

    ObjectNode expected = mapper.createObjectNode();
    expected.put("test", Integer.MAX_VALUE);
    assertEquals(expected.toString(), result.toString());
  }

  @Test
  public void testConnectJsonConverter_INT32_Time_Thread_Safe()
      throws ExecutionException, InterruptedException {

    Callable<Boolean> task = () -> testSimpleDataFormat_jsonConverter_thread_safe();

    // pool with 5 threads
    ExecutorService exec = Executors.newFixedThreadPool(5);
    List<Future<Boolean>> results = new ArrayList<>();

    // perform 10 date conversions
    for (int i = 0; i < 50; i++) {
      results.add(exec.submit(task));
    }
    exec.shutdown();

    // look at the results
    for (Future<Boolean> result : results) {
      assertTrue(result.get());
    }
  }

  private boolean testSimpleDataFormat_jsonConverter_thread_safe() {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", true);

    jsonConverter.configure(config, false);

    String value =
        "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Date\","
            + " \"version\": 1 }, \"payload\": 10000 }";
    SchemaAndValue schemaInputValue = jsonConverter.toConnectData("test", value.getBytes());

    JsonNode result =
        RecordService.convertToJson(schemaInputValue.schema(), schemaInputValue.value(), false);
    System.out.println("Record Service result:" + result + " Thread :" + Thread.currentThread());

    String exptectedDateTimeFormatStr =
        ISO_DATE_TIME_FORMAT.get().format(CALENDAR_THREAD_SAFE.get().getTime());
    return result.toString().contains(exptectedDateTimeFormatStr);
  }

  @Test
  public void testConnectJsonConverter_MapBigDecimalExceedsMaxPrecision()
      throws JsonProcessingException {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("test", new BigDecimal("999999999999999999999999999999999999999"));
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("test", mapper.writeValueAsBytes(jsonMap));
    RecordService.convertToJson(schemaAndValue.schema(), schemaAndValue.value(), false);

    ObjectNode expected = mapper.createObjectNode();
    expected.put("test", new BigDecimal("999999999999999999999999999999999999999"));
    // TODO: uncomment it once KAFKA-10457 is merged
    // assertEquals(expected.toString(), result.toString());
  }

  @Test
  public void testConnectSimpleHeaderConverter_MapDateAndOtherTypes() throws ParseException {
    SimpleHeaderConverter headerConverter = new SimpleHeaderConverter();
    String timestamp = "1970-03-22T00:00:00.000Z";
    String rawHeader = "{\"f1\": \"" + timestamp + "\", \"f2\": true}";
    SchemaAndValue schemaAndValue =
        headerConverter.toConnectHeader(
            "test", "h1", rawHeader.getBytes(StandardCharsets.US_ASCII));
    JsonNode result =
        RecordService.convertToJson(schemaAndValue.schema(), schemaAndValue.value(), false);

    ObjectNode expected = mapper.createObjectNode();
    String expectedTimestampValue =
        String.valueOf(
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .parse("1970-03-22T00:00:00.000Z")
                .getTime());
    expected.put("f1", expectedTimestampValue);
    expected.put("f2", true);
    assertEquals(expected.toString(), result.toString());
  }
}
