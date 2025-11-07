package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceV1.FormattingUtils.formatName;
import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SnowflakeConnectionServiceV1Test {

  @ParameterizedTest
  @CsvSource({"role, ROLE", "Role, ROLE", "\"role\", role", "\"rOle\", rOle"})
  void testFormatNames(String inputName, String expectedName) {
    assertThat(formatName(inputName)).isEqualTo(expectedName);
  }
}
