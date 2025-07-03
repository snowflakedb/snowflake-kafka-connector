package com.snowflake.kafka.connector.streaming.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.jupiter.api.*;

public class IcebergInitServiceIT extends BaseIcebergIT {

  private static IcebergInitService icebergInitService;

  private String tableName;

  @BeforeAll
  // overrides the base class @BeforeAll
  public static void setup() {
    conn = TestUtils.getConnectionServiceForStreaming();
    icebergInitService = new IcebergInitService(conn);
  }

  @BeforeEach
  public void setUp() {
    tableName = TestUtils.randomTableName();
  }

  @AfterEach
  public void tearDown() {
    dropIcebergTable(tableName);
  }

  @Test
  void shouldInitializeMetadataType() {
    // given
    createIcebergTable(tableName);

    // when
    icebergInitService.initializeIcebergTableProperties(tableName);

    // then
    assertThat(describeRecordMetadataType(tableName))
        .isEqualTo(
            "OBJECT(offset NUMBER(19,0), "
                + "topic VARCHAR(134217728), "
                + "partition NUMBER(10,0), "
                + "key VARCHAR(134217728), "
                + "schema_id NUMBER(10,0), "
                + "key_schema_id NUMBER(10,0), "
                + "CreateTime NUMBER(19,0), "
                + "LogAppendTime NUMBER(19,0), "
                + "SnowflakeConnectorPushTime NUMBER(19,0), "
                + "headers MAP(VARCHAR(134217728), "
                + "VARCHAR(134217728)))");
  }

  @Test
  void shouldMigrateOffsetFromIntToLong() {
    // given
    createIcebergTableWithColumnClause(
        tableName,
        "record_metadata OBJECT(offset INTEGER, topic STRING, partition INTEGER, key STRING,"
            + " schema_id INTEGER, key_schema_id INTEGER, CreateTime BIGINT, LogAppendTime BIGINT,"
            + " SnowflakeConnectorPushTime BIGINT, headers MAP(VARCHAR, VARCHAR))");

    // when
    icebergInitService.initializeIcebergTableProperties(tableName);

    // then
    assertThat(describeRecordMetadataType(tableName))
        .isEqualTo(
            "OBJECT(offset NUMBER(19,0), "
                + "topic VARCHAR(134217728), "
                + "partition NUMBER(10,0), "
                + "key VARCHAR(134217728), "
                + "schema_id NUMBER(10,0), "
                + "key_schema_id NUMBER(10,0), "
                + "CreateTime NUMBER(19,0), "
                + "LogAppendTime NUMBER(19,0), "
                + "SnowflakeConnectorPushTime NUMBER(19,0), "
                + "headers MAP(VARCHAR(134217728), "
                + "VARCHAR(134217728)))");
  }

  @Test
  void shouldThrowExceptionWhenTableDoesNotExist() {
    assertThatThrownBy(() -> icebergInitService.initializeIcebergTableProperties(tableName))
        .isInstanceOf(SnowflakeKafkaConnectorException.class);
  }

  @Test
  void shouldCreateMetadataWhenColumnNotExists() {
    // given
    createIcebergTableWithColumnClause(tableName, "some_column VARCHAR");

    // when
    icebergInitService.initializeIcebergTableProperties(tableName);

    // then
    assertThat(describeRecordMetadataType(tableName))
        .isEqualTo(
            "OBJECT(offset NUMBER(19,0), "
                + "topic VARCHAR(134217728), "
                + "partition NUMBER(10,0), "
                + "key VARCHAR(134217728), "
                + "schema_id NUMBER(10,0), "
                + "key_schema_id NUMBER(10,0), "
                + "CreateTime NUMBER(19,0), "
                + "LogAppendTime NUMBER(19,0), "
                + "SnowflakeConnectorPushTime NUMBER(19,0), "
                + "headers MAP(VARCHAR(134217728), "
                + "VARCHAR(134217728)))");
  }
}
