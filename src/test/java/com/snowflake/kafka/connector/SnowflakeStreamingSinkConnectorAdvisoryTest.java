package com.snowflake.kafka.connector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SnowflakeStreamingSinkConnector#hasCritical(List)}. */
public class SnowflakeStreamingSinkConnectorAdvisoryTest {

  @Test
  public void hasCritical_emptyList_returnsFalse() {
    assertFalse(SnowflakeStreamingSinkConnector.hasCritical(Collections.emptyList()));
  }

  @Test
  public void hasCritical_onlyInfoAndWarn_returnsFalse() {
    List<AdvisoryMessage> advisories =
        Arrays.asList(
            new AdvisoryMessage("INFO", "all good"), new AdvisoryMessage("WARN", "heads up"));
    assertFalse(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }

  @Test
  public void hasCritical_containsCritical_returnsTrue() {
    List<AdvisoryMessage> advisories =
        Arrays.asList(
            new AdvisoryMessage("WARN", "heads up"),
            new AdvisoryMessage("CRITICAL", "you must upgrade"));
    assertTrue(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }

  @Test
  public void hasCritical_criticalCaseInsensitive_returnsTrue() {
    List<AdvisoryMessage> advisories =
        Collections.singletonList(new AdvisoryMessage("critical", "lower-case critical"));
    assertTrue(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }

  @Test
  public void hasCritical_unknownLevelDefaultsToWarn_returnsFalse() {
    // Unknown levels default to WARN, so they must not trigger the critical path.
    List<AdvisoryMessage> advisories =
        Collections.singletonList(new AdvisoryMessage("UNKNOWN_LEVEL", "some message"));
    assertFalse(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }
}
