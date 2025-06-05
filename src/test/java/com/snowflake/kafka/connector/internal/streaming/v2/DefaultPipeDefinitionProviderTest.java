package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DefaultPipeDefinitionProviderTest {

  private static final PipeDefinitionProvider provider = new DefaultPipeDefinitionProvider();

  @ParameterizedTest
  @MethodSource("testParams")
  void shouldCreatePipeDefinition(boolean recreate, String expected) {
    // when
    String result = provider.getPipeDefinition("foo", recreate);

    // then
    assertThat(result).isEqualTo(expected);
  }

  public static Stream<Arguments> testParams() {
    return Stream.of(
        Arguments.of(
            true,
            "CREATE OR REPLACE PIPE identifier(?) AS COPY INTO foo FROM TABLE (DATA_SOURCE(TYPE =>"
                + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE"),
        Arguments.of(
            false,
            "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO foo FROM TABLE (DATA_SOURCE(TYPE"
                + " => 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE"));
  }
}
