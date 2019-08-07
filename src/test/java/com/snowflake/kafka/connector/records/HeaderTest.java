package com.snowflake.kafka.connector.records;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HeaderTest
{
  private final static ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testTypes() throws IOException
  {
    Headers headers = new ConnectHeaders();

    //empty headers
    SinkRecord record = createTestRecord(headers);
    RecordService service = new RecordService();
    JsonNode node = MAPPER.readTree(service.processRecord(record));
    assert !node.get("meta").has("headers");

    //primitive types
    String byteName = "byte";
    byte byteData = -24;
    String shortName = "short";
    short shortData = 128;
    String intName = "int";
    int intData = 32767;
    String longName = "long";
    long longData = 1234567890123L;
    String floatName = "float";
    float floatData = 1.234f;
    String doubleName = "double";
    double doubleData = 123.23432532345;
    String booleanName = "boolean";
    boolean booleanData = true;
    String stringName = "string";
    String stringData = "test test";
    String bytesName = "bytes";
    byte[] bytesData = {1, 2, 3, 4, 5, 6, 7, 8};

    headers.addByte(byteName, byteData);
    headers.addShort(shortName, shortData);
    headers.addInt(intName, intData);
    headers.addFloat(floatName, floatData);
    headers.addDouble(doubleName, doubleData);
    headers.addLong(longName, longData);
    headers.addBoolean(booleanName, booleanData);
    headers.addString(stringName, stringData);
    headers.addBytes(bytesName, bytesData);

    record = createTestRecord(headers);
    node = MAPPER.readTree(service.processRecord(record));

    assert node.get("meta").has("headers");
    JsonNode headerNode = node.get("meta").get("headers");

    assert headerNode.has(byteName);
    assert headerNode.get(byteName).asInt() == byteData;
    assert headerNode.has(shortName);
    assert headerNode.get(shortName).asInt() == shortData;
    assert headerNode.has(intName);
    assert headerNode.get(intName).asInt() == intData;
    assert headerNode.has(longName);
    assert headerNode.get(longName).asLong() == longData;
    assert headerNode.has(floatName);
    assert headerNode.get(floatName).floatValue() == floatData;
    assert headerNode.has(doubleName);
    assert headerNode.get(doubleName).asDouble() == doubleData;
    assert headerNode.has(booleanName);
    assert headerNode.get(booleanName).asBoolean() == booleanData;
    assert headerNode.has(stringName);
    assert headerNode.get(stringName).asText().equals(stringData);
    assert headerNode.has(bytesName);
    assert Arrays.equals(headerNode.get(bytesName).binaryValue(), bytesData);

    //array
    headers = new ConnectHeaders();
    String arrayName = "array";
    List<Integer> array = new LinkedList<>();
    array.add(0);
    array.add(1);
    array.add(2);
    headers.addList(arrayName, array,
      SchemaBuilder.array(Schema.INT32_SCHEMA).build());
    record = createTestRecord(headers);
    node = MAPPER.readTree(service.processRecord(record));
    assert node.get("meta").has("headers");
    headerNode = node.get("meta").get("headers");
    assert headerNode.has(arrayName);
    assert headerNode.get(arrayName).isArray();
    int i = 0;
    for (JsonNode element : headerNode.get(arrayName))
    {
      assert element.asInt() == array.get(i);
      i++;
    }

    //map
    headers = new ConnectHeaders();
    String mapName = "map";
    Map<String, Boolean> map = new HashMap<>();
    String mapKey1 = "key1";
    boolean mapValue1 = true;
    String mapKey2 = "key2";
    boolean mapValue2 = false;
    map.put(mapKey1, mapValue1);
    map.put(mapKey2, mapValue2);
    headers.addMap(mapName, map, SchemaBuilder.map(Schema.STRING_SCHEMA,
      Schema.BOOLEAN_SCHEMA));
    record = createTestRecord(headers);
    node = MAPPER.readTree(service.processRecord(record));
    assert node.get("meta").has("headers");
    headerNode = node.get("meta").get("headers");
    assert headerNode.has(mapName);
    assert headerNode.get(mapName).has(mapKey1);
    assert headerNode.get(mapName).get(mapKey1).asBoolean() == mapValue1;
    assert headerNode.get(mapName).has(mapKey2);
    assert headerNode.get(mapName).get(mapKey2).asBoolean() == mapValue2;
    i = 0;
    Iterator<String> names = headerNode.get(mapName).fieldNames();
    while(names.hasNext())
    {
      i++;
      names.next();
    }
    assert i == 2;

    //struct
    headers = new ConnectHeaders();
    String structName = "struct";
    String key1 = "key1";
    double value1 = 123.456;
    String key2 = "key2";
    long value2 = 1234567890L;
    Struct struct = new Struct(
      SchemaBuilder.struct()
        .field(key1, Schema.FLOAT64_SCHEMA)
        .field(key2, Schema.INT64_SCHEMA)
        .build()
    );
    struct.put(key1, value1);
    struct.put(key2, value2);
    headers.addStruct(structName, struct);
    record = createTestRecord(headers);
    node = MAPPER.readTree(service.processRecord(record));
    assert node.get("meta").has("headers");
    headerNode = node.get("meta").get("headers");
    assert headerNode.has(structName);
    assert headerNode.get(structName).has(key1);
    assert headerNode.get(structName).get(key1).asDouble() == value1;
    assert headerNode.get(structName).has(key2);
    assert headerNode.get(structName).get(key2).asLong() == value2;


  }

  private static SinkRecord createTestRecord(Headers headers) throws IOException
  {
    return new SinkRecord("test-topic", 0, Schema.STRING_SCHEMA, "key",
      new SnowflakeJsonSchema(), new SnowflakeRecordContent(MAPPER.readTree(
      "{\"num\":123}")), 0, System.currentTimeMillis(),
      TimestampType.CREATE_TIME, headers);
  }
}
