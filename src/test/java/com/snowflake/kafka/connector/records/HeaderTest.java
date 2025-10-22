package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class HeaderTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static final SimpleDateFormat ISO_DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSSZ");

  static {
    ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Test
  public void testTypes() throws IOException {
    Headers headers = new ConnectHeaders();

    // empty headers
    SinkRecord record = createTestRecord(headers);
    RecordService service = RecordServiceFactory.createRecordService(false, false);
    JsonNode node = MAPPER.readTree(service.getProcessedRecordForSnowpipe(record));
    assertFalse(node.get("meta").has("headers"));

    // primitive types
    String byteName = "byte";
    byte byteData = -24;
    String shortName = "short";
    short shortData = 128;
    String intName = "int";
    int intData = Integer.MAX_VALUE;
    String longName = "long";
    long longData = Long.MAX_VALUE;
    String floatName = "float";
    float floatData = 1 / 3f;
    String doubleName = "double";
    double doubleData = 1 / 3d;
    String booleanName = "boolean";

    boolean booleanData = true;
    String stringName = "string";
    String stringData = "test test";
    String bytesName = "bytes";
    byte[] bytesData = {1, 2, 3, 4, 5, 6, 7, 8};
    String bigDecimalName = "bigDecimal";
    BigDecimal bigDecimalData = new BigDecimal("1234.1234");

    String bigDecimalExceedsMaxPrecisionName = "bigDecimalExceedsMaxPrecision";
    BigDecimal bigDecimalExceedsMaxPrecisionData =
        new BigDecimal("999999999999999999999999999999999999999");

    String dateName = "date";
    Date dateData = new Date(1577836800000L);
    String timeName = "time";
    Date timeData = new Date(54321L);
    String timestampName = "timestamp";
    Date timestampData = new Date(1577836854321L);

    headers.addByte(byteName, byteData);
    headers.addShort(shortName, shortData);
    headers.addInt(intName, intData);
    headers.addFloat(floatName, floatData);
    headers.addDouble(doubleName, doubleData);
    headers.addLong(longName, longData);
    headers.addBoolean(booleanName, booleanData);
    headers.addString(stringName, stringData);
    headers.addBytes(bytesName, bytesData);
    headers.addDecimal(bigDecimalName, bigDecimalData);
    headers.addDecimal(bigDecimalExceedsMaxPrecisionName, bigDecimalExceedsMaxPrecisionData);
    headers.addDate(dateName, dateData);
    headers.addTime(timeName, timeData);
    headers.addTimestamp(timestampName, timestampData);

    record = createTestRecord(headers);
    node = MAPPER.readTree(service.getProcessedRecordForSnowpipe(record));

    assertTrue(node.get("meta").has("headers"));
    JsonNode headerNode = node.get("meta").get("headers");

    assertTrue(headerNode.has(byteName));
    assertEquals(byteData, headerNode.get(byteName).asInt());
    assertTrue(headerNode.has(shortName));
    assertEquals(shortData, headerNode.get(shortName).asInt());
    assertTrue(headerNode.has(intName));
    assertEquals(intData, headerNode.get(intName).asInt());
    assertTrue(headerNode.has(longName));
    assertEquals(longData, headerNode.get(longName).asLong());
    assertTrue(headerNode.has(floatName));
    assertEquals(floatData, headerNode.get(floatName).floatValue());
    assertTrue(headerNode.has(doubleName));
    assertEquals(doubleData, headerNode.get(doubleName).asDouble());
    assertTrue(headerNode.has(booleanName));
    assertEquals(booleanData, headerNode.get(booleanName).asBoolean());
    assertTrue(headerNode.has(stringName));
    assertEquals(stringData, headerNode.get(stringName).asText());
    assertTrue(headerNode.has(bytesName));
    assertArrayEquals(bytesData, headerNode.get(bytesName).binaryValue());
    assertTrue(headerNode.has(bigDecimalName));
    assertEquals(bigDecimalData, headerNode.get(bigDecimalName).decimalValue());
    assertTrue(headerNode.has(dateName));
    assertEquals(ISO_DATE_FORMAT.format(dateData), headerNode.get(dateName).asText());
    assertTrue(headerNode.has(timeName));
    assertEquals(TIME_FORMAT.format(timeData), headerNode.get(timeName).asText());
    assertTrue(headerNode.has(timestampName));
    assertEquals(timestampData.getTime(), headerNode.get(timestampName).asLong());
    assertEquals(
        bigDecimalExceedsMaxPrecisionData.toString(),
        headerNode.get(bigDecimalExceedsMaxPrecisionName).asText());

    // array
    headers = new ConnectHeaders();
    String arrayName = "array";
    List<Integer> array = new LinkedList<>();
    array.add(0);
    array.add(1);
    array.add(2);
    headers.addList(arrayName, array, SchemaBuilder.array(Schema.INT32_SCHEMA).build());
    record = createTestRecord(headers);
    node = MAPPER.readTree(service.getProcessedRecordForSnowpipe(record));
    assertTrue(node.get("meta").has("headers"));
    headerNode = node.get("meta").get("headers");
    assertTrue(headerNode.has(arrayName));
    assertTrue(headerNode.get(arrayName).isArray());
    int i = 0;
    for (JsonNode element : headerNode.get(arrayName)) {
      assertEquals(array.get(i), element.asInt());
      i++;
    }

    // map
    headers = new ConnectHeaders();
    String mapName = "map";
    Map<String, Boolean> map = new HashMap<>();
    String mapKey1 = "key1";
    boolean mapValue1 = true;
    String mapKey2 = "key2";
    boolean mapValue2 = false;
    map.put(mapKey1, mapValue1);
    map.put(mapKey2, mapValue2);
    headers.addMap(mapName, map, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA));
    record = createTestRecord(headers);
    node = MAPPER.readTree(service.getProcessedRecordForSnowpipe(record));
    assertTrue(node.get("meta").has("headers"));
    headerNode = node.get("meta").get("headers");
    assertTrue(headerNode.has(mapName));
    assertTrue(headerNode.get(mapName).has(mapKey1));
    assertEquals(mapValue1, headerNode.get(mapName).get(mapKey1).asBoolean());
    assertTrue(headerNode.get(mapName).has(mapKey2));
    assertEquals(mapValue2, headerNode.get(mapName).get(mapKey2).asBoolean());
    i = 0;
    Iterator<String> names = headerNode.get(mapName).fieldNames();
    while (names.hasNext()) {
      i++;
      names.next();
    }
    assertEquals(2, i);

    // struct
    headers = new ConnectHeaders();
    String structName = "struct";
    String key1 = "key1";
    double value1 = 123.456;
    String key2 = "key2";
    long value2 = 1234567890L;
    Struct struct =
        new Struct(
            SchemaBuilder.struct()
                .field(key1, Schema.FLOAT64_SCHEMA)
                .field(key2, Schema.INT64_SCHEMA)
                .build());
    struct.put(key1, value1);
    struct.put(key2, value2);
    headers.addStruct(structName, struct);
    record = createTestRecord(headers);
    node = MAPPER.readTree(service.getProcessedRecordForSnowpipe(record));
    assertTrue(node.get("meta").has("headers"));
    headerNode = node.get("meta").get("headers");
    assertTrue(headerNode.has(structName));
    assertTrue(headerNode.get(structName).has(key1));
    assertEquals(value1, headerNode.get(structName).get(key1).asDouble());
    assertTrue(headerNode.get(structName).has(key2));
    assertEquals(value2, headerNode.get(structName).get(key2).asLong());
  }

  private static SinkRecord createTestRecord(Headers headers) throws IOException {
    return new SinkRecord(
        "test-topic",
        0,
        Schema.STRING_SCHEMA,
        "key",
        new SnowflakeJsonSchema(),
        new SnowflakeRecordContent(MAPPER.readTree("{\"num\":123}")),
        0,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME,
        headers);
  }
}
