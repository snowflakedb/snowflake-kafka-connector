package com.snowflake.kafka.connector.internal.validation;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import org.junit.jupiter.api.Test;

public class ValidationUtilitiesTest {

  @Test
  public void testErrorMessagesResourceLoading() {
    // Verify that the error messages properties file is loadable
    ResourceBundle bundle = ResourceBundle.getBundle(ErrorCode.errorMessageResource);
    assertNotNull(bundle, "Error messages resource bundle should be loadable");

    // Verify key error messages exist
    assertNotNull(bundle.getString("0001"), "INTERNAL_ERROR message should exist");
    assertNotNull(bundle.getString("0004"), "INVALID_FORMAT_ROW message should exist");
    assertNotNull(bundle.getString("0005"), "UNKNOWN_DATA_TYPE message should exist");
    assertNotNull(bundle.getString("0020"), "IO_ERROR message should exist");
    assertNotNull(bundle.getString("0029"), "UNSUPPORTED_DATA_TYPE message should exist");
    assertNotNull(bundle.getString("0030"), "INVALID_VALUE_ROW message should exist");
  }

  @Test
  public void testSFExceptionCreation() {
    SFException ex = new SFException(ErrorCode.INVALID_FORMAT_ROW, "test column", "test value");

    assertNotNull(ex.getMessage());
    assertEquals("0004", ex.getVendorCode());
    assertTrue(ex.isErrorCode(ErrorCode.INVALID_FORMAT_ROW));
    assertFalse(ex.isErrorCode(ErrorCode.INTERNAL_ERROR));
  }

  @Test
  public void testBinaryStringUtils() {
    assertEquals(5, BinaryStringUtils.unicodeCharactersCount("hello"));
    assertEquals(3, BinaryStringUtils.unicodeCharactersCount("你好世"));
    assertEquals(0, BinaryStringUtils.unicodeCharactersCount(""));

    // Test with emoji (surrogate pairs)
    assertEquals(2, BinaryStringUtils.unicodeCharactersCount("👋🌍"));
  }

  @Test
  public void testLiteralQuoteUtils() {
    // Test unquoted identifier
    assertEquals("COLUMNNAME", LiteralQuoteUtils.unquoteColumnName("columnName"));

    // Test quoted identifier
    assertEquals("columnName", LiteralQuoteUtils.unquoteColumnName("\"columnName\""));

    // Test with escaped double quotes
    assertEquals("col\"name", LiteralQuoteUtils.unquoteColumnName("\"col\"\"name\""));

    // Test with escaped spaces in unquoted name - gets uppercased
    assertEquals("COL NAME", LiteralQuoteUtils.unquoteColumnName("col\\ name"));

    // Test empty string
    assertEquals("", LiteralQuoteUtils.unquoteColumnName(""));
  }

  @Test
  public void testUtilsStripTrailingNulls() {
    assertEquals("hello", Utils.stripTrailingNulls("hello"));
    assertEquals("hello", Utils.stripTrailingNulls("hello\u0000"));
    assertEquals("hello", Utils.stripTrailingNulls("hello\u0000\u0000\u0000"));
    assertEquals("", Utils.stripTrailingNulls("\u0000\u0000"));
    assertEquals("", Utils.stripTrailingNulls(""));
    assertEquals("hel\u0000lo", Utils.stripTrailingNulls("hel\u0000lo\u0000"));
  }

  @Test
  public void testZonedDateTimeSerializer() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
    mapper.registerModule(module);

    ZonedDateTime dt = ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("UTC"));
    String json = mapper.writeValueAsString(dt);

    assertNotNull(json);
    assertTrue(json.contains("2024"));
  }

  @Test
  public void testByteArraySerializer() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(byte[].class, new ByteArraySerializer());
    mapper.registerModule(module);

    byte[] data = new byte[]{1, 2, 3, 4, 5};
    String json = mapper.writeValueAsString(data);

    assertNotNull(json);
    // ByteArraySerializer converts to JSON array of numbers
    assertTrue(json.startsWith("["));
    assertTrue(json.endsWith("]"));
    assertTrue(json.contains("1"));
  }

  @Test
  public void testDuplicateKeyValidatingSerializer() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(DuplicateKeyValidatedObject.class, new DuplicateKeyValidatingSerializer());
    mapper.registerModule(module);

    // Test with map - wrapped in DuplicateKeyValidatedObject
    Map<String, Object> data = new HashMap<>();
    data.put("key1", "value1");
    data.put("key2", "value2");
    DuplicateKeyValidatedObject obj = new DuplicateKeyValidatedObject(data);

    String json = mapper.writeValueAsString(obj);
    assertNotNull(json);
  }

  @Test
  public void testDuplicateKeyValidatedObject() {
    Map<String, Object> data = new HashMap<>();
    data.put("key", "value1");

    DuplicateKeyValidatedObject obj = new DuplicateKeyValidatedObject(data);
    assertEquals(data, obj.getObject());
  }

  @Test
  public void testDuplicateDetector() {
    DuplicateDetector<String> detector = new DuplicateDetector<>();

    // First occurrence should return false (not a duplicate)
    assertFalse(detector.isDuplicate("key1"));

    // Second occurrence should return true (is a duplicate)
    assertTrue(detector.isDuplicate("key1"));

    // Different key should return false
    assertFalse(detector.isDuplicate("key2"));

    // Another duplicate
    assertTrue(detector.isDuplicate("key2"));
  }

  @Test
  public void testColumnLogicalType() {
    assertEquals(1, ColumnLogicalType.BOOLEAN.getOrdinal());
    assertEquals(2, ColumnLogicalType.FIXED.getOrdinal());
    assertEquals(9, ColumnLogicalType.TEXT.getOrdinal());

    // Test composite types
    assertTrue(ColumnLogicalType.ARRAY.isObject());
    assertTrue(ColumnLogicalType.OBJECT.isObject());
    assertTrue(ColumnLogicalType.VARIANT.isObject());
    assertFalse(ColumnLogicalType.TEXT.isObject());
  }

  @Test
  public void testColumnPhysicalType() {
    assertEquals(1, ColumnPhysicalType.SB1.getOrdinal());
    assertEquals(7, ColumnPhysicalType.DOUBLE.getOrdinal());
    assertEquals(8, ColumnPhysicalType.LOB.getOrdinal());
  }

  @Test
  public void testTimestampWrapper() {
    ZonedDateTime zdt = ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 123456789, ZoneId.of("UTC"));
    TimestampWrapper wrapper = new TimestampWrapper(zdt.toOffsetDateTime(), 9);

    assertEquals(zdt.toEpochSecond(), wrapper.getEpochSecond());
    assertEquals(123456789, wrapper.getFraction());
    assertEquals(0, wrapper.getTimezoneOffsetSeconds()); // UTC
    assertEquals(1440, wrapper.getTimeZoneIndex()); // 1440 is UTC
  }

  @Test
  public void testTimestampWrapperWithNonUTCTimezone() {
    ZonedDateTime zdt = ZonedDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneId.of("America/Los_Angeles"));
    TimestampWrapper wrapper = new TimestampWrapper(zdt.toOffsetDateTime(), 6);

    assertNotEquals(0, wrapper.getTimezoneOffsetSeconds());
    assertNotEquals(1440, wrapper.getTimeZoneIndex());
  }

  @Test
  public void testTimestampWrapperInvalidScale() {
    ZonedDateTime zdt = ZonedDateTime.now();

    assertThrows(IllegalArgumentException.class, () -> new TimestampWrapper(zdt.toOffsetDateTime(), -1));
    assertThrows(IllegalArgumentException.class, () -> new TimestampWrapper(zdt.toOffsetDateTime(), 10));
  }
}
