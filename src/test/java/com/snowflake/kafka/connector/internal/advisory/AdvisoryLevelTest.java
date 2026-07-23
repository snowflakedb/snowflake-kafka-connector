package com.snowflake.kafka.connector.internal.advisory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AdvisoryLevelTest {

  @Test
  public void mapsKnownLevelsCaseInsensitively() {
    assertEquals(AdvisoryLevel.INFO, AdvisoryLevel.fromString("INFO"));
    assertEquals(AdvisoryLevel.WARN, AdvisoryLevel.fromString("warn"));
    assertEquals(AdvisoryLevel.ERROR, AdvisoryLevel.fromString("Error"));
  }

  @Test
  public void unknownOrNullDefaultsToWarn() {
    assertEquals(AdvisoryLevel.WARN, AdvisoryLevel.fromString("bogus"));
    assertEquals(AdvisoryLevel.WARN, AdvisoryLevel.fromString(null));
  }
}
