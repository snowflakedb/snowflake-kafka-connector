package com.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;

/**
 * Copy of {@code net.snowflake.ingest.streaming.internal.TimestampWrapper} from
 * snowflake-ingest-sdk used by {@link
 * com.snowflake.kafka.connector.internal.streaming.validation.PkgDataValidationUtil} to serialize
 * timestamp values without depending on the legacy SDK artifact.
 */
public class TimestampWrapper {

  private final long epoch;
  private final int fraction;
  private final int timezoneOffsetSeconds;
  private final int scale;

  private static final int BITS_FOR_TIMEZONE = 14;
  private static final int MASK_OF_TIMEZONE = (1 << BITS_FOR_TIMEZONE) - 1;

  public TimestampWrapper(OffsetDateTime offsetDateTime, int scale) {
    if (scale < 0 || scale > 9) {
      throw new IllegalArgumentException(
          String.format("Scale must be between 0 and 9, actual: %d", scale));
    }
    this.epoch = offsetDateTime.toEpochSecond();
    this.fraction =
        offsetDateTime.getNano() / Power10.intTable[9 - scale] * Power10.intTable[9 - scale];
    this.timezoneOffsetSeconds = offsetDateTime.getOffset().getTotalSeconds();
    this.scale = scale;
  }

  /** Convert the timestamp to a binary representation. */
  public BigInteger toBinary(boolean includeTimezone) {
    BigDecimal timeInNs =
        BigDecimal.valueOf(epoch).scaleByPowerOfTen(9).add(new BigDecimal(fraction));
    BigDecimal scaledTime = timeInNs.scaleByPowerOfTen(scale - 9);
    scaledTime = scaledTime.setScale(0, RoundingMode.DOWN);
    BigInteger fcpInt = scaledTime.unscaledValue();
    if (includeTimezone) {
      int offsetMin = timezoneOffsetSeconds / 60;
      offsetMin += 1440;
      fcpInt = fcpInt.shiftLeft(BITS_FOR_TIMEZONE);
      fcpInt = fcpInt.add(BigInteger.valueOf(offsetMin & MASK_OF_TIMEZONE));
    }
    return fcpInt;
  }

  public long getEpochSecond() {
    return epoch;
  }

  public int getFraction() {
    return fraction;
  }

  public int getTimezoneOffsetSeconds() {
    return timezoneOffsetSeconds;
  }

  public int getTimeZoneIndex() {
    return timezoneOffsetSeconds / 60 + 1440;
  }
}
