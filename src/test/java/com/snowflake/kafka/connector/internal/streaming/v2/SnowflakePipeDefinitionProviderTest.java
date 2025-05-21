package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.internal.DescribeTableRow;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SnowflakePipeDefinitionProviderTest {

  private static final PipeDefinitionProvider provider = new SnowflakePipeDefinitionProvider();

  @ParameterizedTest
  @MethodSource("testParams")
  void shouldCreatePipeDefinition(boolean recreate, String expected) {
    // given
    List<DescribeTableRow> describeTableRows =
        List.of(
            new DescribeTableRow("RECORD_METADATA", "VARIANT"),
            new DescribeTableRow("RECORD_CONTENT", "VARIANT"));

    // when
    String result = provider.getPipeDefinition("foo", describeTableRows, recreate);

    // then
    assertThat(result).isEqualTo(expected);
  }

  public static Stream<Arguments> testParams() {
    return Stream.of(
        Arguments.of(
            true,
            "CREATE OR REPLACE PIPE identifier(?) AS COPY INTO foo FROM (SELECT "
                + "PARSE_JSON($1:RECORD_METADATA), PARSE_JSON($1:RECORD_CONTENT) "
                + "FROM TABLE (DATA_SOURCE(TYPE => 'STREAMING')))"),
        Arguments.of(
            false,
            "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO foo FROM (SELECT "
                + "PARSE_JSON($1:RECORD_METADATA), PARSE_JSON($1:RECORD_CONTENT) "
                + "FROM TABLE (DATA_SOURCE(TYPE => 'STREAMING')))"));
  }
}
