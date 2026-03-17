package com.snowflake.kafka.connector.internal.validation;

import java.math.BigInteger;

/**
 * Powers of 10 used for timestamp/time scaling and validation. Replicates the semantics of
 * Snowflake JDBC internal Power10 so the connector does not depend on JDBC internal APIs (removed
 * in JDBC 4.x).
 */
public final class Power10Util {

  private Power10Util() {}

  /** Size of the power tables (10^0 through 10^9). */
  public static final int sb16Size = 10;

  /** 10^i as int for i in [0, 9]. Used for timestamp fraction scaling. */
  public static final int[] intTable = new int[sb16Size];

  /** 10^i as BigInteger for i in [0, 9]. Used for time/timestamp validation and scaling. */
  public static final BigInteger[] sb16Table = new BigInteger[sb16Size];

  static {
    for (int i = 0; i < sb16Size; i++) {
      intTable[i] = (int) Math.pow(10, i);
      sb16Table[i] = BigInteger.TEN.pow(i);
    }
  }
}
