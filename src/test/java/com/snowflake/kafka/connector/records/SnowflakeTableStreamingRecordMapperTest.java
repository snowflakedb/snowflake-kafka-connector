package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SnowflakeTableStreamingRecordMapperTest extends StreamingRecordMapperTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  // Helper constants for metadata structures
  private static final Map<String, Object> EMPTY_SSV2_METADATA = Map.of("headers", Map.of());

  private static final Map<String, Object> FULL_SSV2_METADATA =
      Map.of(
          "offset",
          10,
          "topic",
          "topic",
          "partition",
          0,
          "key",
          "key",
          "schema_id",
          1,
          "key_schema_id",
          2,
          "CreateTime",
          3,
          "LogAppendTime",
          4,
          "SnowflakeConnectorPushTime",
          5,
          "headers",
          Map.of(
              "header2",
              "testheaderstring",
              "header3",
              "3.5",
              "objectAsJsonStringHeader",
              "{\"key1\":\"value1\",\"key2\":\"value2\"}"));

  public static Stream<Arguments> ssv2Data() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(buildRow("{}", "{}"), Map.of(TABLE_COLUMN_METADATA, EMPTY_SSV2_METADATA)),
        Arguments.of(
            buildRowWithDefaultMetadata("{}"), Map.of(TABLE_COLUMN_METADATA, FULL_SSV2_METADATA)),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": \"value\"}"),
            Map.of(TABLE_COLUMN_METADATA, FULL_SSV2_METADATA, "key", "value")),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": []}"),
            Map.of(TABLE_COLUMN_METADATA, FULL_SSV2_METADATA, "key", List.of())));
  }

  @ParameterizedTest
  @MethodSource("ssv2Data")
  public void shouldMapDataForSsv2Schematization(
      RecordService.SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // given
    SnowflakeTableStreamingRecordMapper mapper =
        new SnowflakeTableStreamingRecordMapper(objectMapper);

    // when
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);

    // then
    assertThat(result).isEqualTo(expected);
  }
}
