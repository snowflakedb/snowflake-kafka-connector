package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord.primitiveJsonExample;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.RecordService.SnowflakeTableRow;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IcebergTableStreamingRecordMapperTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final ImmutableMap<String, Object> primitiveJsonAsMap =
      ImmutableMap.of(
          "id_int8",
          8,
          "id_int16",
          16,
          "id_int32",
          32,
          "id_int64",
          64,
          "description",
          "dogs are the best",
          "rating_float32",
          0.5,
          "rating_float64",
          0.25,
          "approval",
          true);

  private static final String fullMetadataJsonExample =
      "{"
          + "\"offset\": 10,"
          + "\"topic\": \"topic\","
          + "\"partition\": 0,"
          + "\"key\": \"key\","
          + "\"schema_id\": 1,"
          + "\"key_schema_id\": 2,"
          + "\"CreateTime\": 3,"
          + "\"LogAppendTime\": 4,"
          + "\"SnowflakeConnectorPushTime\": 5,"
          + "\"headers\": {\"objectAsJsonStringHeader\": {"
          + "\"key1\": \"value1\","
          + "\"key2\": \"value2\""
          + "},"
          + "\"header2\": \"testheaderstring\","
          + "\"header3\": 3.5}"
          + "}";

  private static final Map<String, Object> fullMetadataJsonAsMap =
      ImmutableMap.of(
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
          ImmutableMap.of(
              "objectAsJsonStringHeader",
              "{\"key1\":\"value1\",\"key2\":\"value2\"}",
              "header2",
              "testheaderstring",
              "header3",
              "3.5"));

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareSchematizationData")
  void shouldMapRecord_schematizationEnabled(
      String description, SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // When
    IcebergTableStreamingRecordMapper mapper =
        new IcebergTableStreamingRecordMapper(objectMapper, true);
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareMetadataData")
  void shouldMapMetadata(String description, SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // When
    IcebergTableStreamingRecordMapper mapper =
        new IcebergTableStreamingRecordMapper(objectMapper, false);
    IcebergTableStreamingRecordMapper mapperSchematization =
        new IcebergTableStreamingRecordMapper(objectMapper, true);
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);
    Map<String, Object> resultSchematized = mapperSchematization.processSnowflakeRecord(row, true);

    // Then
    assertThat(result.get(Utils.TABLE_COLUMN_METADATA)).isEqualTo(expected);
    assertThat(resultSchematized.get(Utils.TABLE_COLUMN_METADATA)).isEqualTo(expected);
  }

  @Test
  void shouldSkipMapMetadata() throws JsonProcessingException {
    // Given
    SnowflakeTableRow row = buildRow(primitiveJsonExample);

    // When
    IcebergTableStreamingRecordMapper mapper =
        new IcebergTableStreamingRecordMapper(objectMapper, false);
    IcebergTableStreamingRecordMapper mapperSchematization =
        new IcebergTableStreamingRecordMapper(objectMapper, true);
    Map<String, Object> result = mapper.processSnowflakeRecord(row, false);
    Map<String, Object> resultSchematized = mapperSchematization.processSnowflakeRecord(row, false);

    // Then
    assertThat(result).doesNotContainKey(Utils.TABLE_COLUMN_METADATA);
    assertThat(resultSchematized).doesNotContainKey(Utils.TABLE_COLUMN_METADATA);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareNoSchematizationData")
  void shouldMapRecord_schematizationDisabled(
      String description, SnowflakeTableRow row, Map<String, Object> expected)
      throws JsonProcessingException {
    // When
    IcebergTableStreamingRecordMapper mapper =
        new IcebergTableStreamingRecordMapper(objectMapper, false);
    Map<String, Object> result = mapper.processSnowflakeRecord(row, true);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> prepareSchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            "Empty JSON",
            buildRow("{}"),
            ImmutableMap.of(Utils.TABLE_COLUMN_METADATA, fullMetadataJsonAsMap)),
        Arguments.of(
            "Simple JSON",
            buildRow("{\"key\": \"value\"}"),
            ImmutableMap.of(
                "\"KEY\"", "value", Utils.TABLE_COLUMN_METADATA, fullMetadataJsonAsMap)),
        Arguments.of(
            "Already quoted key JSON",
            buildRow("{\"\\\"key\\\"\": \"value\"}"),
            ImmutableMap.of(
                "\"key\"", "value", Utils.TABLE_COLUMN_METADATA, fullMetadataJsonAsMap)),
        Arguments.of(
            "Already quoted UPPERCASE key JSON",
            buildRow("{\"\\\"KEY\\\"\": \"value\"}"),
            ImmutableMap.of(
                "\"KEY\"", "value", Utils.TABLE_COLUMN_METADATA, fullMetadataJsonAsMap)),
        Arguments.of(
            "JSON with different primitive types",
            buildRow(primitiveJsonExample),
            ImmutableMap.of(
                "\"ID_INT8\"",
                8,
                "\"ID_INT16\"",
                16,
                "\"ID_INT32\"",
                32,
                "\"ID_INT64\"",
                64,
                "\"DESCRIPTION\"",
                "dogs are the best",
                "\"RATING_FLOAT32\"",
                0.5,
                "\"RATING_FLOAT64\"",
                0.25,
                "\"APPROVAL\"",
                true,
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Nested array",
            buildRow("{\"key\": [" + primitiveJsonExample + ", " + primitiveJsonExample + "]}"),
            ImmutableMap.of(
                "\"KEY\"",
                ImmutableList.of(primitiveJsonAsMap, primitiveJsonAsMap),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Empty nested array",
            buildRow("{\"key\": []}"),
            ImmutableMap.of(
                "\"KEY\"", ImmutableList.of(), Utils.TABLE_COLUMN_METADATA, fullMetadataJsonAsMap)),
        Arguments.of(
            "Empty object",
            buildRow("{\"key\": {}}"),
            ImmutableMap.of(
                "\"KEY\"", ImmutableMap.of(), Utils.TABLE_COLUMN_METADATA, fullMetadataJsonAsMap)),
        Arguments.of(
            "Nested object",
            buildRow("{\"key\":" + primitiveJsonExample + "}"),
            ImmutableMap.of(
                "\"KEY\"", primitiveJsonAsMap, Utils.TABLE_COLUMN_METADATA, fullMetadataJsonAsMap)),
        Arguments.of(
            "Double nested object",
            buildRow("{\"key\":{\"key2\":" + primitiveJsonExample + "}}"),
            ImmutableMap.of(
                "\"KEY\"",
                ImmutableMap.of("key2", primitiveJsonAsMap),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Nested objects and primitive types",
            buildRow(
                primitiveJsonExample.replaceFirst("}", "")
                    + ",\"key\":"
                    + primitiveJsonExample
                    + "}"),
            ImmutableMap.of(
                "\"ID_INT8\"",
                8,
                "\"ID_INT16\"",
                16,
                "\"ID_INT32\"",
                32,
                "\"ID_INT64\"",
                64,
                "\"DESCRIPTION\"",
                "dogs are the best",
                "\"RATING_FLOAT32\"",
                0.5,
                "\"RATING_FLOAT64\"",
                0.25,
                "\"APPROVAL\"",
                true,
                "\"KEY\"",
                primitiveJsonAsMap,
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)));
  }

  private static Stream<Arguments> prepareMetadataData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of("Full metadata", buildRow("{}"), fullMetadataJsonAsMap),
        Arguments.of(
            "Empty metadata", buildRow("{}", "{}"), ImmutableMap.of("headers", ImmutableMap.of())),
        Arguments.of(
            "Metadata with null headers",
            buildRow("{}", "{\"headers\": null}"),
            ImmutableMap.of("headers", ImmutableMap.of())),
        Arguments.of(
            "Metadata with empty headers",
            buildRow("{}", "{\"headers\": {}}"),
            ImmutableMap.of("headers", ImmutableMap.of())),
        Arguments.of(
            "Metadata with headers with null keys",
            buildRow("{}", "{\"headers\": {\"key\": null}}"),
            ImmutableMap.of("headers", mapWithNullableValuesOf("key", null))),
        Arguments.of(
            "Metadata with headers with nested null keys",
            buildRow("{}", "{\"headers\": {\"key\": {\"key2\": null }}}"),
            ImmutableMap.of("headers", ImmutableMap.of("key", "{\"key2\":null}"))),
        Arguments.of(
            "Metadata with null field value",
            buildRow("{}", "{\"offset\": null}"),
            mapWithNullableValuesOf("offset", null, "headers", ImmutableMap.of())));
  }

  private static Stream<Arguments> prepareNoSchematizationData() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            "Empty JSON",
            buildRow("{}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of(),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Simple JSON",
            buildRow("{\"key\": \"value\"}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of("key", "value"),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "UPPERCASE key JSON",
            buildRow("{\"KEY\": \"value\"}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of("KEY", "value"),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "JSON with different primitive types",
            buildRow(primitiveJsonExample),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                primitiveJsonAsMap,
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Nested array",
            buildRow("{\"key\": [" + primitiveJsonExample + ", " + primitiveJsonExample + "]}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of("key", ImmutableList.of(primitiveJsonAsMap, primitiveJsonAsMap)),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Empty nested array",
            buildRow("{\"key\": []}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of("key", ImmutableList.of()),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Empty object",
            buildRow("{\"key\": {}}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of("key", ImmutableMap.of()),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Nested object",
            buildRow("{\"key\":" + primitiveJsonExample + "}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of("key", primitiveJsonAsMap),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Double nested object",
            buildRow("{\"key\":{\"key2\":" + primitiveJsonExample + "}}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                ImmutableMap.of("key", ImmutableMap.of("key2", primitiveJsonAsMap)),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)),
        Arguments.of(
            "Nested objects and primitive types",
            buildRow(
                primitiveJsonExample.replaceFirst("}", "")
                    + ",\"key\":"
                    + primitiveJsonExample
                    + "}"),
            ImmutableMap.of(
                Utils.TABLE_COLUMN_CONTENT,
                addToMap(primitiveJsonAsMap, "key", primitiveJsonAsMap),
                Utils.TABLE_COLUMN_METADATA,
                fullMetadataJsonAsMap)));
  }

  private static <T> Map<String, T> addToMap(Map<String, T> map, String key, T value) {
    HashMap<String, T> newMap = new HashMap<>(map);
    newMap.put(key, value);
    return newMap;
  }

  private static <T> Map<String, ?> mapWithNullableValuesOf(String key, T value) {
    Map<String, T> map = new HashMap<>();
    map.put(key, value);
    return map;
  }

  private static <T> Map<String, ?> mapWithNullableValuesOf(
      String key, T value, String key2, T value2) {
    Map<String, T> map = new HashMap<>();
    map.put(key, value);
    map.put(key2, value2);
    return map;
  }

  private static SnowflakeTableRow buildRow(String content) throws JsonProcessingException {
    return buildRow(content, fullMetadataJsonExample);
  }

  private static SnowflakeTableRow buildRow(String content, String metadata)
      throws JsonProcessingException {
    return new SnowflakeTableRow(
        new SnowflakeRecordContent(objectMapper.readTree(content)),
        objectMapper.readTree(metadata));
  }
}
