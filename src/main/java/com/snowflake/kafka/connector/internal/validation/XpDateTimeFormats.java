package com.snowflake.kafka.connector.internal.validation;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Client-side mirror of the SSv2 XP server's accepted date/time format allow-list.
 *
 * <p>Source of truth: trunk ExecPlatform/.../snowflake_date_lib/core/date/TimestampFormatd.hpp
 * (TIME lines 273-306). Each formatter is annotated with the server format-ID/line it mirrors.
 * The ONLY TZ-bearing TIME format requires fractional seconds; offset-without-fraction TIME is
 * intentionally rejected to match XP (SNOW-3766306).
 */
final class XpDateTimeFormats {

  private XpDateTimeFormats() {}

  private static final List<DateTimeFormatter> TIME_FORMATS =
      Arrays.asList(
          // ISO_HOUR24_MINUTE_SECOND_FRAC_TZ  "HH24:MI:SS.FFTZH:TZM" (line 273)
          new DateTimeFormatterBuilder()
              .appendPattern("HH:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .appendOffsetId()
              .toFormatter(Locale.ROOT),
          // ISO_HOUR24_MINUTE_SECOND_FRAC     "HH24:MI:SS.FF" (line 278)
          new DateTimeFormatterBuilder()
              .appendPattern("HH:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .toFormatter(Locale.ROOT),
          // RFC_HOUR12_MINUTE_SECOND_FRAC_MERIDIEM "HH12:MI:SS.FF AM" (line ~284)
          new DateTimeFormatterBuilder()
              .appendPattern("hh:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .appendPattern(" a")
              .toFormatter(Locale.ENGLISH),
          // ISO_HOUR24_MINUTE_SECOND          "HH24:MI:SS" (line ~290)
          DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT),
          // RFC_HOUR12_MINUTE_SECOND_MERIDIEM "HH12:MI:SS AM" (line ~294)
          DateTimeFormatter.ofPattern("hh:mm:ss a", Locale.ENGLISH),
          // ISO_HOUR24_MINUTE                 "HH24:MI" (line ~298)
          DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT),
          // RFC_HOUR12_MINUTE_MERIDIEM        "HH12:MI AM" (line ~302)
          DateTimeFormatter.ofPattern("hh:mm a", Locale.ENGLISH));

  static Optional<LocalTime> tryParseTime(String input) {
    String s = input.trim();
    for (DateTimeFormatter f : TIME_FORMATS) {
      try {
        return Optional.of(LocalTime.parse(s, f));
      } catch (DateTimeParseException ignored) {
        // try next format
      }
    }
    return Optional.empty();
  }
}
