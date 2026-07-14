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
}
