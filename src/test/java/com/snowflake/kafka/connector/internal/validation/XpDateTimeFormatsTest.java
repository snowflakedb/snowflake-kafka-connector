package com.snowflake.kafka.connector.internal.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.LocalTime;
import org.junit.Test;

public class XpDateTimeFormatsTest {

  @Test
  public void time_acceptsServerFormats() {
    assertTrue(XpDateTimeFormats.tryParseTime("00:00:00").isPresent());
    assertTrue(XpDateTimeFormats.tryParseTime("07:59:59.999999").isPresent());
    assertTrue(XpDateTimeFormats.tryParseTime("07:59:59.999999Z").isPresent()); // .FF + TZ
    assertTrue(XpDateTimeFormats.tryParseTime("22:00").isPresent());
    assertEquals(
        LocalTime.of(7, 59, 59, 999999000),
        XpDateTimeFormats.tryParseTime("07:59:59.999999").get());
  }

  @Test
  public void time_rejectsOffsetWithoutFraction() {
    assertFalse(XpDateTimeFormats.tryParseTime("00:00:00Z").isPresent());
    assertFalse(XpDateTimeFormats.tryParseTime("22:00:00Z").isPresent());
    assertFalse(XpDateTimeFormats.tryParseTime("22:00Z").isPresent());
    assertFalse(XpDateTimeFormats.tryParseTime("22:00:00+05:00").isPresent());
  }

  @Test
  public void timestamp_acceptsIsoTandSpaceVariants() {
    java.time.ZoneId la = java.time.ZoneId.of("America/Los_Angeles");
    assertTrue(XpDateTimeFormats.tryParseTimestamp("2024-01-15T10:30:00", la).isPresent());
    assertTrue(XpDateTimeFormats.tryParseTimestamp("2024-01-15T10:30:00.123Z", la).isPresent());
    assertTrue(
        XpDateTimeFormats.tryParseTimestamp("2024-01-15T10:30:00+05:30", la).isPresent());
    assertTrue(XpDateTimeFormats.tryParseTimestamp("2024-01-15 10:30:00", la).isPresent());
    assertTrue(
        XpDateTimeFormats.tryParseTimestamp("2024-01-15 10:30:00.123", la).isPresent());
    assertTrue(
        XpDateTimeFormats.tryParseTimestamp("2024-01-15 10:30:00 +05:30", la).isPresent());
    assertTrue(XpDateTimeFormats.tryParseTimestamp("2024-01-15", la).isPresent());
  }

  @Test
  public void date_acceptsIsoAndDatetime() {
    java.time.ZoneId utc = java.time.ZoneOffset.UTC;
    assertTrue(XpDateTimeFormats.tryParseDate("2024-01-15", utc).isPresent());
    assertTrue(XpDateTimeFormats.tryParseDate("2024-01-15T10:30:00Z", utc).isPresent());
    assertTrue(XpDateTimeFormats.tryParseDate("2024-01-15 10:30:00", utc).isPresent());
  }

  @Test
  public void timestamp_acceptsHumanFormats() {
    java.time.ZoneId utc = java.time.ZoneOffset.UTC;
    assertTrue(XpDateTimeFormats.tryParseTimestamp("15-Jan-2024", utc).isPresent());
    assertTrue(XpDateTimeFormats.tryParseTimestamp("01/15/2024", utc).isPresent());
    assertTrue(XpDateTimeFormats.tryParseTimestamp("01/15/2024 10:30:00", utc).isPresent());
  }

  @Test
  public void formatCountMatchesServerList() {
    assertEquals(XpDateTimeFormats.EXPECTED_TS_FORMAT_COUNT, XpDateTimeFormats.tsFormatCount());
  }
}
