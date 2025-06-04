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
            buildRow("{}"),
            Map.of(
                TABLE_COLUMN_METADATA, fullMetadataWithoutWhitespace, TABLE_COLUMN_CONTENT, "{}")),
        Arguments.of(
            buildRow("{\"key\": \"value\"}"),
            Map.of(
                TABLE_COLUMN_METADATA,
                fullMetadataWithoutWhitespace,
                TABLE_COLUMN_CONTENT,
                "{\"key\":\"value\"}")));
  }

  public static Stream<Arguments> ssv1SchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(buildRow("{}", "{}"), Map.of(TABLE_COLUMN_METADATA, "{}")),
        Arguments.of(buildRow("{}"), Map.of(TABLE_COLUMN_METADATA, fullMetadataWithoutWhitespace)),
        Arguments.of(
            buildRow("{\"key\": \"value\"}"),
            Map.of(TABLE_COLUMN_METADATA, fullMetadataWithoutWhitespace, "\"KEY\"", "value")),
        Arguments.of(
            buildRow("{\"key\": []}"),
            Map.of(
                TABLE_COLUMN_METADATA, fullMetadataJsonExampleWithoutWhitespace, "\"KEY\"", "[]")));
  }

  public static Stream<Arguments> ssv2NoSchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            buildRow("{}", "{}"),
            Map.of(
                TABLE_COLUMN_METADATA,
                new MetadataRecord(null, null, null, null, null, null, null, null, null, Map.of()),
                TABLE_COLUMN_CONTENT,
                new HashMap<>())),
        Arguments.of(
            buildRow("{}"),
            Map.of(
                TABLE_COLUMN_METADATA, fullRecordMetadata, TABLE_COLUMN_CONTENT, new HashMap<>())),
        Arguments.of(
            buildRow("{\"key\": \"value\"}"),
            Map.of(
                TABLE_COLUMN_METADATA,
                fullRecordMetadata,
                TABLE_COLUMN_CONTENT,
                Map.of("key", "value"))));
  }

  public static Stream<Arguments> ssv2SchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            buildRow("{}", "{}"),
            Map.of(
                TABLE_COLUMN_METADATA,
                new MetadataRecord(
                    null, null, null, null, null, null, null, null, null, Map.of()))),
        Arguments.of(buildRow("{}"), Map.of(TABLE_COLUMN_METADATA, fullRecordMetadata)),
        Arguments.of(
            buildRow("{\"key\": \"value\"}"),
            Map.of(TABLE_COLUMN_METADATA, fullRecordMetadata, "\"KEY\"", "value")),
        Arguments.of(
            buildRow("{\"key\": []}"),
            Map.of(TABLE_COLUMN_METADATA, fullRecordMetadata, "\"KEY\"", List.of())));
  }
}
