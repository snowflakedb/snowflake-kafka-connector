package com.snowflake.kafka.connector.internal.validation;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Client-side mirror of the SSv2 XP server's accepted date/time format allow-list.
 *
 * <p>Source of truth: trunk ExecPlatform/.../snowflake_date_lib/core/date/TimestampFormatd.hpp
 * (TIME lines 273-306, TIMESTAMP/DATE lines 70-318). Each formatter is annotated with the server
 * format-ID/line it mirrors. The ONLY TZ-bearing TIME format requires fractional seconds; offset-
 * without-fraction TIME is intentionally rejected to match XP (SNOW-3766306).
 */
final class XpDateTimeFormats {

  private XpDateTimeFormats() {}

  // -------------------------------------------------------------------------
  // Helper: build a DateTimeFormatter with optional fractional seconds and TZ offset
  // -------------------------------------------------------------------------
  private enum OffsetStyle {
    NONE,
    COLON,
    NOCOLON,
    SPACE_COLON,
    SPACE_NOCOLON,
    HOURONLY
  }

  private static DateTimeFormatter dt(String pattern, boolean frac, OffsetStyle off) {
    DateTimeFormatterBuilder b = new DateTimeFormatterBuilder().appendPattern(pattern);
    b.parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
     .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);
    if (frac) {
      b.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true);
    } else {
      b.parseDefaulting(ChronoField.NANO_OF_SECOND, 0);
    }
    switch (off) {
      case COLON:
        b.appendOffset("+HH:MM", "Z");
        break;
      case NOCOLON:
        b.appendOffset("+HHMM", "Z");
        break;
      case SPACE_COLON:
        b.appendLiteral(' ').appendOffset("+HH:MM", "Z");
        break;
      case SPACE_NOCOLON:
        b.appendLiteral(' ').appendOffset("+HHMM", "Z");
        break;
      case HOURONLY:
        b.appendOffset("+HH", "Z");
        break;
      case NONE:
        break;
    }
    return b.toFormatter(Locale.ROOT);
  }

  // -------------------------------------------------------------------------
  // TIME formats (lines 273-306 in TimestampFormatd.hpp) — TIME-only entries
  // -------------------------------------------------------------------------
  private static final List<DateTimeFormatter> TIME_FORMATS =
      Arrays.asList(
          // ISO_HOUR24_MINUTE_SECOND_FRAC_TZ  "HH24:MI:SS.FFTZH:TZM" (line 273)
          new DateTimeFormatterBuilder()
              .appendPattern("HH:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .appendOffset("+HH:MM", "Z")
              .toFormatter(Locale.ROOT),
          // ISO_HOUR24_MINUTE_SECOND_FRAC     "HH24:MI:SS.FF" (line 278)
          new DateTimeFormatterBuilder()
              .appendPattern("HH:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .toFormatter(Locale.ROOT),
          // RFC_HOUR12_MINUTE_SECOND_FRAC_MERIDIEM "HH12:MI:SS.FF AM" (line 284)
          new DateTimeFormatterBuilder()
              .appendPattern("hh:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .appendPattern(" a")
              .toFormatter(Locale.ENGLISH),
          // ISO_HOUR24_MINUTE_SECOND          "HH24:MI:SS" (line 289)
          DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT),
          // RFC_HOUR12_MINUTE_SECOND_MERIDIEM "HH12:MI:SS AM" (line 294)
          DateTimeFormatter.ofPattern("hh:mm:ss a", Locale.ENGLISH),
          // ISO_HOUR24_MINUTE                 "HH24:MI" (line 299)
          DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT),
          // RFC_HOUR12_MINUTE_MERIDIEM        "HH12:MI AM" (line 304)
          DateTimeFormatter.ofPattern("hh:mm a", Locale.ENGLISH));

  // -------------------------------------------------------------------------
  // TIMESTAMP/DATE formats (lines 75-318 in TimestampFormatd.hpp)
  // Entries with LogicalType::TIMESTAMP_* or DATE in their type tag set.
  // TIME-only entries (lines 273-306) are handled above; NOT duplicated here.
  //
  // UNMAPPED entries (cannot be expressed in java.time):
  //   SNOWFLAKE_SDL_TIMESTAMPTZ (line 70): uses internal "Ztz=TZIDX" TZ-index encoding
  //   TWITTER_DATE_HOUR24_MIN_SEC_TZ_YEAR (line 267): "DY MON DD HH24:MI:SS TZHTZM YYYY"
  //     — year at end; java.time parseBest cannot easily handle year-at-end reordering;
  //       excluded. See UNMAPPED comment below.
  // -------------------------------------------------------------------------

  // RFC 2822-style day-of-week abbreviations + comma: "EEE, dd MMM yyyy HH:mm:ss"
  // The server's DY = abbreviated English weekday (Mon/Tue/Wed/Thu/Fri/Sat/Sun).
  // These ARE expressible in java.time with Locale.ENGLISH.

  private static DateTimeFormatter rfc(String pattern, boolean frac, boolean withTz) {
    DateTimeFormatterBuilder b = new DateTimeFormatterBuilder().appendPattern(pattern);
    if (frac) b.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true);
    if (withTz) b.appendLiteral(' ').appendOffset("+HHMM", "Z");
    return b.toFormatter(Locale.ENGLISH);
  }

  // EXPECTED_TS_FORMAT_COUNT: number of formatters in TS_FORMATS.
  // Update this constant whenever TS_FORMATS changes (drift guard).
  static final int EXPECTED_TS_FORMAT_COUNT = 32;

  static int tsFormatCount() {
    return TS_FORMATS.size();
  }

  private static final List<DateTimeFormatter> TS_FORMATS =
      Arrays.asList(
          // --- SDL aliases (same pattern as ISO entries below, but distinct server IDs) ---

          // SNOWFLAKE_SDL_TIMESTAMP (line 75): "UUUU-MM-DD HH24:MI:SS.FFTZH:TZM"
          // TIMESTAMP_LTZ, TIMESTAMP_NTZ — same as ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZHCM
          // NOTE: kept as a distinct entry to match server ordering; deduplication is harmless.
          dt("yyyy-MM-dd HH:mm:ss", true, OffsetStyle.COLON), // SDL_TIMESTAMP (line 75)

          // SNOWFLAKE_SDL_DATE (line 80): "UUUU-MM-DD" — DATE only
          DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT), // SDL_DATE / ISO_DATE (80/178)

          // --- ISO T-separator variants ---

          // ISO_DATE_T_HOUR24_MINUTE_SECOND_FRAC_TZHM (line 88): "YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM"
          dt("yyyy-MM-dd'T'HH:mm:ss", true, OffsetStyle.COLON),

          // ISO_DATE_T_HOUR24_MINUTE_SECOND_FRAC (line 130): "YYYY-MM-DD\"T\"HH24:MI:SS.FF"
          dt("yyyy-MM-dd'T'HH:mm:ss", true, OffsetStyle.NONE),

          // ISO_DATE_T_HOUR24_MINUTE_SECOND_TZHCM (line 190): "YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM"
          dt("yyyy-MM-dd'T'HH:mm:ss", false, OffsetStyle.COLON),

          // ISO_DATE_T_HOUR24_MINUTE_SECOND (line 142): "YYYY-MM-DD\"T\"HH24:MI:SS"
          dt("yyyy-MM-dd'T'HH:mm:ss", false, OffsetStyle.NONE),

          // ISO_DATE_T_HOUR24_MINUTE_TZHCM (line 208): "YYYY-MM-DD\"T\"HH24:MITZH:TZM"
          dt("yyyy-MM-dd'T'HH:mm", false, OffsetStyle.COLON),

          // ISO_DATE_HOUR24_MINUTE (line 154): "YYYY-MM-DD\"T\"HH24:MI"
          dt("yyyy-MM-dd'T'HH:mm", false, OffsetStyle.NONE),

          // ISO_DATE_T_HOUR24 (line 166): "YYYY-MM-DD\"T\"HH24"
          dt("yyyy-MM-dd'T'HH", false, OffsetStyle.NONE),

          // --- ISO space-separator variants ---

          // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZHCM (line 94): "YYYY-MM-DD HH24:MI:SS.FFTZH:TZM"
          // (already added as SDL_TIMESTAMP above; add only if distinct offset style needed)
          // Already covered at index 0 — skip to avoid duplicate.

          // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZH (line 100): "YYYY-MM-DD HH24:MI:SS.FFTZH"
          dt("yyyy-MM-dd HH:mm:ss", true, OffsetStyle.HOURONLY),

          // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZSHCM (line 106): "YYYY-MM-DD HH24:MI:SS.FF TZH:TZM"
          dt("yyyy-MM-dd HH:mm:ss", true, OffsetStyle.SPACE_COLON),

          // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZSHM (line 112): "YYYY-MM-DD HH24:MI:SS.FF TZHTZM"
          dt("yyyy-MM-dd HH:mm:ss", true, OffsetStyle.SPACE_NOCOLON),

          // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC2 (line 136): "YYYY-MM-DD HH24:MI:SS.FF"
          dt("yyyy-MM-dd HH:mm:ss", true, OffsetStyle.NONE),

          // ISO_DATE_HOUR24_MINUTE_SECOND_TZSHCM (line 118): "YYYY-MM-DD HH24:MI:SS TZH:TZM"
          dt("yyyy-MM-dd HH:mm:ss", false, OffsetStyle.SPACE_COLON),

          // ISO_DATE_HOUR24_MINUTE_SECOND_TZSHM (line 124): "YYYY-MM-DD HH24:MI:SS TZHTZM"
          dt("yyyy-MM-dd HH:mm:ss", false, OffsetStyle.SPACE_NOCOLON),

          // ISO_DATE_HOUR24_MINUTE_SECOND_TZHCM (line 196): "YYYY-MM-DD HH24:MI:SSTZH:TZM"
          dt("yyyy-MM-dd HH:mm:ss", false, OffsetStyle.COLON),

          // ISO_DATE_HOUR24_MINUTE_SECOND_TZH (line 202): "YYYY-MM-DD HH24:MI:SSTZH"
          dt("yyyy-MM-dd HH:mm:ss", false, OffsetStyle.HOURONLY),

          // ISO_DATE_HOUR24_MINUTE_SECOND2 (line 148): "YYYY-MM-DD HH24:MI:SS"
          dt("yyyy-MM-dd HH:mm:ss", false, OffsetStyle.NONE),

          // ISO_DATE_HOUR24_MINUTE_TZHCM (line 214): "YYYY-MM-DD HH24:MITZH:TZM"
          dt("yyyy-MM-dd HH:mm", false, OffsetStyle.COLON),

          // ISO_DATE_HOUR24_MINUTE2 (line 160): "YYYY-MM-DD HH24:MI"
          dt("yyyy-MM-dd HH:mm", false, OffsetStyle.NONE),

          // ISO_DATE_HOUR24_2 (line 172): "YYYY-MM-DD HH24"
          dt("yyyy-MM-dd HH", false, OffsetStyle.NONE),

          // ISO_US_SIMPLE_DATE (line 184): "DD-MON-YYYY" e.g. "17-DEC-1980"
          DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH),

          // --- RFC 2822 / email-header variants (lines 220-265) ---
          // DY = abbreviated English weekday e.g. "Thu"

          // RFC_DATE_HOUR24_MINUTE_SECOND_TZ (line 220): "DY, DD MON YYYY HH24:MI:SS TZHTZM"
          rfc("EEE, dd MMM yyyy HH:mm:ss", false, true),

          // RFC_DATE_HOUR24_MINUTE_SECOND_FRAC_TZ (line 226): "DY, DD MON YYYY HH24:MI:SS.FF TZHTZM"
          rfc("EEE, dd MMM yyyy HH:mm:ss", true, true),

          // RFC_DATE_HOUR12_MINUTE_SECOND_MERIDIEM_TZ (line 232): "DY, DD MON YYYY HH12:MI:SS AM TZHTZM"
          new DateTimeFormatterBuilder()
              .appendPattern("EEE, dd MMM yyyy hh:mm:ss a")
              .appendLiteral(' ')
              .appendOffset("+HHMM", "Z")
              .toFormatter(Locale.ENGLISH),

          // RFC_DATE_HOUR12_MINUTE_SECOND_FRAC_MERIDIEM_TZ (line 238):
          // "DY, DD MON YYYY HH12:MI:SS.FF AM TZHTZM"
          new DateTimeFormatterBuilder()
              .appendPattern("EEE, dd MMM yyyy hh:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .appendPattern(" a")
              .appendLiteral(' ')
              .appendOffset("+HHMM", "Z")
              .toFormatter(Locale.ENGLISH),

          // RFC_DATE_HOUR24_MINUTE_SECOND (line 244): "DY, DD MON YYYY HH24:MI:SS"
          rfc("EEE, dd MMM yyyy HH:mm:ss", false, false),

          // RFC_DATE_HOUR24_MINUTE_SECOND_FRAC (line 250): "DY, DD MON YYYY HH24:MI:SS.FF"
          rfc("EEE, dd MMM yyyy HH:mm:ss", true, false),

          // RFC_DATE_HOUR12_MINUTE_SECOND_MERIDIEM (line 256): "DY, DD MON YYYY HH12:MI:SS AM"
          DateTimeFormatter.ofPattern("EEE, dd MMM yyyy hh:mm:ss a", Locale.ENGLISH),

          // RFC_DATE_HOUR12_MINUTE_SECOND_FRAC_MERIDIEM (line 262): "DY, DD MON YYYY HH12:MI:SS.FF AM"
          new DateTimeFormatterBuilder()
              .appendPattern("EEE, dd MMM yyyy hh:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
              .appendPattern(" a")
              .toFormatter(Locale.ENGLISH),

          // TWITTER_DATE_HOUR24_MIN_SEC_TZ_YEAR (line 267): "DY MON DD HH24:MI:SS TZHTZM YYYY"
          // UNMAPPED: year is at the END of the pattern; java.time parseBest cannot reorder fields
          // arbitrarily and DateTimeFormatter.parse(...) would require a custom resolver.
          // Excluded from TS_FORMATS and EXPECTED_TS_FORMAT_COUNT.

          // --- Deprecated US-style formats (lines 309-318) ---

          // ISO_US_DATE_ALT1 (line 309): "MM/DD/YYYY" e.g. "12/17/1980"
          DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.ROOT),

          // ALT_DATE_HOUR24_MIN_SEC (line 315): "MM/DD/YYYY HH24:MI:SS" e.g. "2/18/2008 02:36:48"
          dt("MM/dd/yyyy HH:mm:ss", false, OffsetStyle.NONE));

  // -------------------------------------------------------------------------
  // Epoch: integer-stored timestamps delegating to DataValidationUtil scale-guesser
  // -------------------------------------------------------------------------
  private static Optional<OffsetDateTime> tryEpoch(String s) {
    if (!s.matches("[+-]?\\d+")) return Optional.empty();
    // Reject values beyond Long.MAX_VALUE range (19 digits max for a positive long).
    // parseInstantGuessScale interprets huge values as nanoseconds via BigInteger, which
    // silently produces absurd far-future/past dates; reject early instead.
    String digits = s.startsWith("+") || s.startsWith("-") ? s.substring(1) : s;
    if (digits.length() > 19) return Optional.empty();
    try {
      return Optional.of(DataValidationUtil.parseInstantGuessScale(s).atOffset(ZoneOffset.UTC));
    } catch (NumberFormatException | java.time.DateTimeException e) {
      return Optional.empty();
    }
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

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

  static Optional<OffsetDateTime> tryParseTimestamp(String input, ZoneId defaultTz) {
    String s = input.trim();
    Optional<OffsetDateTime> epoch = tryEpoch(s);
    if (epoch.isPresent()) return epoch;
    for (DateTimeFormatter f : TS_FORMATS) {
      try {
        TemporalAccessor ta =
            f.parseBest(s, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);
        if (ta instanceof OffsetDateTime) return Optional.of((OffsetDateTime) ta);
        if (ta instanceof LocalDateTime)
          return Optional.of(((LocalDateTime) ta).atZone(defaultTz).toOffsetDateTime());
        if (ta instanceof LocalDate)
          return Optional.of(((LocalDate) ta).atStartOfDay(defaultTz).toOffsetDateTime());
      } catch (DateTimeParseException ignored) {
        // try next format
      }
    }
    return Optional.empty();
  }

}
