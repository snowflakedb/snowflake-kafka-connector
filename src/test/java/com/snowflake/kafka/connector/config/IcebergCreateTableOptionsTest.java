package com.snowflake.kafka.connector.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IcebergCreateTableOptionsTest {

  private static String sql(String raw) {
    return IcebergCreateTableOptions.parse(raw).toSql();
  }

  // --- valid options ---

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "\t\n"})
  void parse_blank_isEmpty(String raw) {
    IcebergCreateTableOptions options = IcebergCreateTableOptions.parse(raw);
    assertThat(options.toSql()).isEmpty();
    assertThat(options.icebergVersion()).isEmpty();
  }

  @Test
  void parse_null_isEmpty() {
    assertThat(sql(null)).isEmpty();
  }

  @Test
  void parse_stringOption_isCanonicalized() {
    assertThat(sql("external_volume='my_vol'")).isEqualTo("EXTERNAL_VOLUME = 'my_vol'");
  }

  @Test
  void parse_multipleOptions_preservesOrder() {
    assertThat(sql("EXTERNAL_VOLUME = 'v'   ICEBERG_VERSION=3 BASE_LOCATION='a/b'"))
        .isEqualTo("EXTERNAL_VOLUME = 'v' ICEBERG_VERSION = 3 BASE_LOCATION = 'a/b'");
  }

  @Test
  void parse_quotedInteger_isAccepted() {
    IcebergCreateTableOptions options = IcebergCreateTableOptions.parse("ICEBERG_VERSION = '3'");
    assertThat(options.toSql()).isEqualTo("ICEBERG_VERSION = 3");
    assertThat(options.icebergVersion()).isEqualTo(Optional.of(3));
  }

  @Test
  void parse_noVersion_hasNoIcebergVersion() {
    assertThat(IcebergCreateTableOptions.parse("EXTERNAL_VOLUME='v'").icebergVersion()).isEmpty();
  }

  @Test
  void parse_versionInsideComment_isNotIcebergVersion() {
    assertThat(
            IcebergCreateTableOptions.parse("COMMENT='ICEBERG_VERSION=3' ICEBERG_VERSION=2")
                .icebergVersion())
        .isEqualTo(Optional.of(2));
  }

  @Test
  void parse_booleanAndKeywordOptions_areCanonicalized() {
    assertThat(
            sql(
                "change_tracking=true enable_data_compaction='FALSE'"
                    + " storage_serialization_policy=compatible"))
        .isEqualTo(
            "CHANGE_TRACKING = TRUE ENABLE_DATA_COMPACTION = FALSE"
                + " STORAGE_SERIALIZATION_POLICY = COMPATIBLE");
  }

  @Test
  void parse_allIntegerOptions() {
    assertThat(sql("DATA_RETENTION_TIME_IN_DAYS=1 MAX_DATA_EXTENSION_TIME_IN_DAYS=14"))
        .isEqualTo("DATA_RETENTION_TIME_IN_DAYS = 1 MAX_DATA_EXTENSION_TIME_IN_DAYS = 14");
  }

  @Test
  void parse_otherStringOptions() {
    assertThat(
            sql(
                "TARGET_FILE_SIZE='64MB' CATALOG_SYNC='my_sync' COMMENT='hi'"
                    + " ICEBERG_MERGE_ON_READ_BEHAVIOR='AUTO'"))
        .isEqualTo(
            "TARGET_FILE_SIZE = '64MB' CATALOG_SYNC = 'my_sync' COMMENT = 'hi'"
                + " ICEBERG_MERGE_ON_READ_BEHAVIOR = 'AUTO'");
  }

  @Test
  void parse_escapedQuoteInString_roundTrips() {
    assertThat(sql("COMMENT='it''s fine'")).isEqualTo("COMMENT = 'it''s fine'");
  }

  @Test
  void parse_clusterBy_isEmittedFirst() {
    assertThat(sql("EXTERNAL_VOLUME='v' cluster by (RECORD_METADATA:\"offset\"::number)"))
        .isEqualTo("CLUSTER BY (RECORD_METADATA:\"offset\"::NUMBER) EXTERNAL_VOLUME = 'v'");
  }

  @Test
  void parse_clusterBy_multipleKeysAndTypeArguments() {
    assertThat(sql("CLUSTER BY ( id , a.b:c::number(38, 0) , \"Quoted\"\"Id\" )"))
        .isEqualTo("CLUSTER BY (id, a.b:c::NUMBER(38, 0), \"Quoted\"\"Id\")");
  }

  @Test
  void parse_outputReparsesToSameOutput() {
    String canonical =
        sql("CLUSTER BY (x:y::varchar) COMMENT='a''b' ICEBERG_VERSION='3' CHANGE_TRACKING=false");
    assertThat(sql(canonical)).isEqualTo(canonical);
  }

  // --- statement delimiters and comments ---

  @ParameterizedTest
  @ValueSource(
      strings = {
        "EXTERNAL_VOLUME='v'; SELECT 1",
        "EXTERNAL_VOLUME='v';",
        "; SELECT 1; --",
        "EXTERNAL_VOLUME='v; SELECT 1'",
        "COMMENT='a';SELECT 1;--'",
      })
  void parse_statementDelimiter_isRejected(String raw) {
    assertThatThrownBy(() -> IcebergCreateTableOptions.parse(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(";");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "EXTERNAL_VOLUME='v' --",
        "EXTERNAL_VOLUME='v' -- catalog = 'X'",
        "EXTERNAL_VOLUME='v' /* x */",
        "EXTERNAL_VOLUME='v' */",
        "EXTERNAL_VOLUME='v' // x",
        "COMMENT='--'",
        "COMMENT='http://x'",
      })
  void parse_commentMarker_isRejected(String raw) {
    assertThatThrownBy(() -> IcebergCreateTableOptions.parse(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("comment");
  }

  // --- malformed quoting and tokens ---

  @ParameterizedTest
  @ValueSource(
      strings = {
        "EXTERNAL_VOLUME='v",
        "EXTERNAL_VOLUME=v'",
        "EXTERNAL_VOLUME='v''",
        "COMMENT='a' 'b'",
        "CLUSTER BY (\"unterminated)",
        "EXTERNAL_VOLUME='a\\' ICEBERG_VERSION=3",
        "COMMENT='line\nbreak'",
        "EXTERNAL_VOLUME=$$v$$",
        "EXTERNAL_VOLUME='v' ICEBERG_VERSION=3 garbage",
        "EXTERNAL_VOLUME",
        "EXTERNAL_VOLUME=",
        "= 'v'",
        "EXTERNAL_VOLUME=='v'",
        "EXTERNAL_VOLUME='v',ICEBERG_VERSION=3",
        "ICEBERG_VERSION=-1",
        "ICEBERG_VERSION=3.5",
        "ICEBERG_VERSION=99999999999",
        "ICEBERG_VERSION='three'",
        "CHANGE_TRACKING=yes",
        "STORAGE_SERIALIZATION_POLICY=FAST",
        "EXTERNAL_VOLUME=my_vol",
        "EXTERNAL_VOLUME=3",
        "COMMENT=\"x\"",
        "EXTERNAL_VOLUME＝'v'",
      })
  void parse_malformedInput_isRejected(String raw) {
    assertThatThrownBy(() -> IcebergCreateTableOptions.parse(raw))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CLUSTER BY id",
        "CLUSTER BY ()",
        "CLUSTER BY (id",
        "CLUSTER BY (id))",
        "CLUSTER BY (id,)",
        "CLUSTER BY (,id)",
        "CLUSTER BY (f(id))",
        "CLUSTER BY ((select 1))",
        "CLUSTER BY (SYSTEM$WAIT(10))",
        "CLUSTER BY ('literal')",
        "CLUSTER BY (a::)",
        "CLUSTER BY (a:)",
        "CLUSTER BY (a b)",
        "CLUSTER (id)",
        "CLUSTER BY (id) CLUSTER BY (id)",
      })
  void parse_malformedClusterBy_isRejected(String raw) {
    assertThatThrownBy(() -> IcebergCreateTableOptions.parse(raw))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- allowlist, duplicates and connector-owned properties ---

  @ParameterizedTest
  @ValueSource(
      strings = {
        "AUTO_REFRESH=true",
        "PARTITION BY (id)",
        "COPY GRANTS",
        "WITH TAG (t = 'v')",
        "ROW ACCESS POLICY p ON (id)",
        "AS SELECT 1",
      })
  void parse_unsupportedOption_isRejected(String raw) {
    assertThatThrownBy(() -> IcebergCreateTableOptions.parse(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Supported options");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "EXTERNAL_VOLUME='a' EXTERNAL_VOLUME='b'",
        "EXTERNAL_VOLUME='a' external_volume='b'",
        "ICEBERG_VERSION=2 Iceberg_Version='3'",
      })
  void parse_duplicateOption_isRejected(String raw) {
    assertThatThrownBy(() -> IcebergCreateTableOptions.parse(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than once");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CATALOG='OTHER'",
        "catalog = 'SNOWFLAKE'",
        "ENABLE_SCHEMA_EVOLUTION=false",
        "Error_Logging = FALSE",
        "EXTERNAL_VOLUME='v' ERROR_LOGGING=false",
      })
  void parse_connectorOwnedOption_isRejected(String raw) {
    assertThatThrownBy(() -> IcebergCreateTableOptions.parse(raw))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("set by the connector");
  }
}
