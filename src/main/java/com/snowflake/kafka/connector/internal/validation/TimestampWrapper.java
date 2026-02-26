/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 *
 * This file is copied from the Snowflake Ingest SDK v1 (streaming/internal/TimestampWrapper.java)
 * to support client-side validation in Kafka Connector v4.
 */

package com.snowflake.kafka.connector.internal.validation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;

/**
 * This class represents the outcome of timestamp parsing and validation. It contains methods needed
 * to serialize timestamps into Parquet.
 */
public class TimestampWrapper {

  /** Epoch seconds */
  private final long epoch;

  /** Fractional part of the second */
  private final int fraction;

  /** Timezone offset in seconds */
  private final int timezoneOffsetSeconds;

  /** Scale of the timestamp column (0-9) */
  private final int scale;

  /**
   * How many bits should be reserver for the timezone part. Needs to be aligned with {@link
   * net.snowflake.client.jdbc.internal.snowflake.common.core.SFTimestamp#BITS_FOR_TIMEZONE}
   */
  private static final int BITS_FOR_TIMEZONE = 14;

  /**
   * Mask of the timezone bits. Needs to be aligned with {@link
   * net.snowflake.client.jdbc.internal.snowflake.common.core.SFTimestamp#MASK_OF_TIMEZONE}
   */
  private static final int MASK_OF_TIMEZONE = (1 << BITS_FOR_TIMEZONE) - 1;

  /** Create a new instance from {@link OffsetDateTime} and its scale. */
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

  /**
   * Convert the timestamp to a binary representation. Needs to be aligned with {@link
   * net.snowflake.client.jdbc.internal.snowflake.common.core.SFTimestamp#toBinary}.
   */
  public BigInteger toBinary(boolean includeTimezone) {
    BigDecimal timeInNs =
        BigDecimal.valueOf(epoch).scaleByPowerOfTen(9).add(new BigDecimal(fraction));
    BigDecimal scaledTime = timeInNs.scaleByPowerOfTen(scale - 9);
    scaledTime = scaledTime.setScale(0, RoundingMode.DOWN);
    BigInteger fcpInt = scaledTime.unscaledValue();
    if (includeTimezone) {
      int offsetMin = timezoneOffsetSeconds / 60;
      assert offsetMin >= -1440 && offsetMin <= 1440;
      offsetMin += 1440;
      fcpInt = fcpInt.shiftLeft(14);
      fcpInt = fcpInt.add(BigInteger.valueOf(offsetMin & MASK_OF_TIMEZONE));
    }
    return fcpInt;
  }

  /** Get epoch in seconds */
  public long getEpochSecond() {
    return epoch;
  }

  /** Get fractional part of a second */
  public int getFraction() {
    return fraction;
  }

  /** Get timezone offset in seconds */
  public int getTimezoneOffsetSeconds() {
    return timezoneOffsetSeconds;
  }

  /**
   * Get timezone index, 1440 means UTC. Calculation needs to be aligned with {@link
   * net.snowflake.client.jdbc.internal.snowflake.common.core.SFTimestamp#toBinary}
   */
  public int getTimeZoneIndex() {
    return timezoneOffsetSeconds / 60 + 1440;
  }
}
