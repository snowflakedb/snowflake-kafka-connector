package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergDDLTypes {

  public static String ICEBERG_METADATA_OBJECT_SCHEMA =
      "OBJECT("
          + "offset LONG,"
          + "topic STRING,"
          + "partition INTEGER,"
          + "key STRING,"
          + "CreateTime BIGINT,"
          + "LogAppendTime BIGINT,"
          + "SnowflakeConnectorPushTime BIGINT,"
          + "headers MAP(VARCHAR, VARCHAR)"
          + ")";

  /**
   * Validates that the actual RECORD_METADATA structured-OBJECT sub-field set exactly matches the
   * connector's expected set ({@link SnowflakeSinkRecord#ICEBERG_METADATA_FIELDS}).
   *
   * <p>For managed-Iceberg v2, the strict typed-OBJECT cast rejects any row whose map is missing a
   * declared field. The connector only emits {@code ICEBERG_METADATA_FIELDS}, so any extra field in
   * the table's OBJECT schema is one the connector never fills — that field would always be absent
   * in every row, causing every row to be rejected at ingest time.
   *
   * <p>Throws {@code ERROR_0034} on any mismatch: missing field(s), extra field(s), or both.
   *
   * @param tableName table name (used in the error message only)
   * @param actualFields field names from the table's RECORD_METADATA OBJECT (compared
   *     case-insensitively)
   * @throws com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException (ERROR_0034) if
   *     any expected field is missing or any extra field is present
   */
  public static void validateRecordMetadataFields(String tableName, Set<String> actualFields) {
    Set<String> actualLower =
        actualFields.stream().map(f -> f.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    Set<String> expectedLower =
        SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS.stream()
            .map(f -> f.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());

    List<String> missing = new ArrayList<>();
    for (String expected : SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS) {
      if (!actualLower.contains(expected.toLowerCase(Locale.ROOT))) {
        missing.add(expected);
      }
    }
    List<String> extra = new ArrayList<>();
    for (String actual : actualFields) {
      if (!expectedLower.contains(actual.toLowerCase(Locale.ROOT))) {
        extra.add(actual);
      }
    }

    if (missing.isEmpty() && extra.isEmpty()) {
      return;
    }

    StringBuilder msg = new StringBuilder();
    msg.append("Existing table '")
        .append(tableName)
        .append(
            "' has a structured-OBJECT RECORD_METADATA column (managed-Iceberg v2) whose"
                + " sub-field set does not exactly match the connector's expected fields.");
    if (!missing.isEmpty()) {
      msg.append(" Missing field(s): ").append(missing).append(".");
    }
    if (!extra.isEmpty()) {
      msg.append(" Extra field(s): ")
          .append(extra)
          .append(
              " — the connector never emits these, so every row's typed-OBJECT cast would"
                  + " reject the row.");
    }
    msg.append(
        " Either add the missing fields or remove the extra fields from the RECORD_METADATA"
            + " OBJECT column definition (e.g. ALTER TABLE ... ALTER COLUMN RECORD_METADATA SET"
            + " DATA TYPE OBJECT(...)), or re-create the table so the connector can auto-create"
            + " it with the correct schema.");
    throw SnowflakeErrors.ERROR_0034.getException(msg.toString());
  }
}
