package com.snowflake.kafka.connector.streaming.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class IcebergDDLTypesTest {

  @Test
  void validateRecordMetadataFields_exactMatch_noThrow() {
    Set<String> exact = new HashSet<>(SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS);
    // Must not throw.
    IcebergDDLTypes.validateRecordMetadataFields("my_table", exact);
  }

  @Test
  void validateRecordMetadataFields_caseInsensitiveMatch_noThrow() {
    // INFORMATION_SCHEMA stores names in uppercase — match should be case-insensitive.
    Set<String> upperCased = new HashSet<>();
    for (String f : SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS) {
      upperCased.add(f.toUpperCase(java.util.Locale.ROOT));
    }
    IcebergDDLTypes.validateRecordMetadataFields("my_table", upperCased);
  }

  @Test
  void validateRecordMetadataFields_missingField_throwsError0034NamingField() {
    Set<String> missing = new HashSet<>(SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS);
    missing.remove("LogAppendTime");

    assertThatThrownBy(() -> IcebergDDLTypes.validateRecordMetadataFields("my_table", missing))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0034")
        .hasMessageContaining("LogAppendTime");
  }

  @Test
  void validateRecordMetadataFields_extraField_throwsError0034NamingField() {
    Set<String> extra = new HashSet<>(SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS);
    extra.add("unexpectedCustomField");

    assertThatThrownBy(() -> IcebergDDLTypes.validateRecordMetadataFields("my_table", extra))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0034")
        .hasMessageContaining("unexpectedCustomField");
  }

  @Test
  void validateRecordMetadataFields_bothMissingAndExtra_throwsNamingBoth() {
    Set<String> mismatched = new HashSet<>(SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS);
    mismatched.remove("headers");
    mismatched.add("legacyField");

    assertThatThrownBy(() -> IcebergDDLTypes.validateRecordMetadataFields("my_table", mismatched))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0034")
        .hasMessageContaining("headers")
        .hasMessageContaining("legacyField");
  }

  @Test
  void validateRecordMetadataFields_emptySet_throwsNamingAllMissing() {
    assertThatThrownBy(
            () -> IcebergDDLTypes.validateRecordMetadataFields("my_table", new HashSet<>()))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0034");

    // All fields should be mentioned as missing.
    assertThatThrownBy(
            () -> IcebergDDLTypes.validateRecordMetadataFields("my_table", new HashSet<>()))
        .hasMessageContaining("offset")
        .hasMessageContaining("topic");
  }

  @Test
  void icebergMetadataObjectSchema_fieldNamesMatchIcebergMetadataFields() {
    // Smoke-check that ICEBERG_METADATA_OBJECT_SCHEMA and ICEBERG_METADATA_FIELDS stay in sync.
    // Count the fields declared in the OBJECT(...) schema by counting commas at top level.
    String schema = IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA;
    assertThat(schema).startsWith("OBJECT(");
    int depth = 0;
    int topLevelCommas = 0;
    for (char c : schema.toCharArray()) {
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (c == ',' && depth == 1) topLevelCommas++;
    }
    int schemaFieldCount = topLevelCommas + 1;
    assertThat(schemaFieldCount)
        .as("ICEBERG_METADATA_OBJECT_SCHEMA field count must equal ICEBERG_METADATA_FIELDS size")
        .isEqualTo(SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS.size());
  }
}
