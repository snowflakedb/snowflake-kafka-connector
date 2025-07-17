package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
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
          "offset", 10,
          "topic", "topic",
          "partition", 0,
          "key", "key",
          "schema_id", 1,
          "key_schema_id", 2,
          "CreateTime", 3,
          "LogAppendTime", 4,
          "SnowflakeConnectorPushTime", 5,
          "headers",
              Map.of(
                  "header2", "testheaderstring",
                  "header3", "3.5",
                  "objectAsJsonStringHeader", "{\"key1\":\"value1\",\"key2\":\"value2\"}"));

  @ParameterizedTest
  @MethodSource("ssv1NoSchematizationData")
  public void shouldMapDataForSsv1(
      RecordService.SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // given
    SnowflakeTableStreamingRecordMapper mapper =
        new SnowflakeTableStreamingRecordMapper(objectMapper, false, false);

    // when
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("ssv1SchematizationData")
  public void shouldMapDataForSsv1Schematization(
      RecordService.SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // given
    SnowflakeTableStreamingRecordMapper mapper =
        new SnowflakeTableStreamingRecordMapper(objectMapper, true, false);

    // when
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("ssv2NoSchematizationData")
  public void shouldMapDataForSsv2(
      RecordService.SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // given
    SnowflakeTableStreamingRecordMapper mapper =
        new SnowflakeTableStreamingRecordMapper(objectMapper, false, true);

    // when
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("ssv2SchematizationData")
  public void shouldMapDataForSsv2Schematization(
      RecordService.SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // given
    SnowflakeTableStreamingRecordMapper mapper =
        new SnowflakeTableStreamingRecordMapper(objectMapper, true, true);

    // when
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);

    // then
    assertThat(result).isEqualTo(expected);
  }

  public static Stream<Arguments> ssv1NoSchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            buildRow("{}", "{}"), Map.of(TABLE_COLUMN_METADATA, "{}", TABLE_COLUMN_CONTENT, "{}")),
        Arguments.of(
            buildRowWithDefaultMetadata("{}"),
            Map.of(
                TABLE_COLUMN_METADATA, fullMetadataWithoutWhitespace, TABLE_COLUMN_CONTENT, "{}")),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": \"value\"}"),
            Map.of(
                TABLE_COLUMN_METADATA,
                fullMetadataWithoutWhitespace,
                TABLE_COLUMN_CONTENT,
                "{\"key\":\"value\"}")));
  }

  public static Stream<Arguments> ssv1SchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(buildRow("{}", "{}"), Map.of(TABLE_COLUMN_METADATA, "{}")),
        Arguments.of(
            buildRowWithDefaultMetadata("{}"),
            Map.of(TABLE_COLUMN_METADATA, fullMetadataWithoutWhitespace)),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": \"value\"}"),
            Map.of(TABLE_COLUMN_METADATA, fullMetadataWithoutWhitespace, "\"KEY\"", "value")),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": []}"),
            Map.of(TABLE_COLUMN_METADATA, fullMetadataWithoutWhitespace, "\"KEY\"", "[]")));
  }

  public static Stream<Arguments> ssv2NoSchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            buildRow("{}", "{}"),
            Map.of(
                TABLE_COLUMN_METADATA, EMPTY_SSV2_METADATA, TABLE_COLUMN_CONTENT, new HashMap<>())),
        Arguments.of(
            buildRowWithDefaultMetadata("{}"),
            Map.of(
                TABLE_COLUMN_METADATA, FULL_SSV2_METADATA, TABLE_COLUMN_CONTENT, new HashMap<>())),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": \"value\"}"),
            Map.of(
                TABLE_COLUMN_METADATA,
                FULL_SSV2_METADATA,
                TABLE_COLUMN_CONTENT,
                Map.of("key", "value"))));
  }

  public static Stream<Arguments> ssv2SchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(buildRow("{}", "{}"), Map.of(TABLE_COLUMN_METADATA, EMPTY_SSV2_METADATA)),
        Arguments.of(
            buildRowWithDefaultMetadata("{}"), Map.of(TABLE_COLUMN_METADATA, FULL_SSV2_METADATA)),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": \"value\"}"),
            Map.of(TABLE_COLUMN_METADATA, FULL_SSV2_METADATA, "\"KEY\"", "value")),
        Arguments.of(
            buildRowWithDefaultMetadata("{\"key\": []}"),
            Map.of(TABLE_COLUMN_METADATA, FULL_SSV2_METADATA, "\"KEY\"", List.of())));
  }
}
