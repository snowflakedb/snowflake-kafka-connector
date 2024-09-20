package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergTableSchemaValidatorIT extends BaseIcebergIT {

  private static final String TEST_ROLE = "testrole_kafka";

  private static IcebergTableSchemaValidator schemaValidator;

  private String tableName;

  @BeforeAll
  // overrides the base class @BeforeAll
  public static void setup() {
    conn = TestUtils.getConnectionServiceForStreaming();
    schemaValidator = new IcebergTableSchemaValidator(conn);
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
  public void shouldValidateExpectedIcebergTableSchema() {
    // given
    createIcebergTable(tableName);
    enableSchemaEvolution(tableName);

    // when, then
    schemaValidator.validateTable(tableName, TEST_ROLE);
  }

  @Test
  public void shouldThrowExceptionWhenTableDoesNotExist() {
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> schemaValidator.validateTable(tableName, TEST_ROLE));
  }

  @Test
  public void shouldThrowExceptionWhenRecordMetadataDoesNotExist() {
    // given
    createIcebergTableWithColumnClause(tableName, "some_column VARCHAR");

    // expect
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> schemaValidator.validateTable(tableName, TEST_ROLE));
  }

  @Test
  public void shouldThrowExceptionWhenRecordMetadataHasInvalidType() {
    // given
    createIcebergTableWithColumnClause(tableName, "record_metadata MAP(VARCHAR, VARCHAR)");

    // expect
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> schemaValidator.validateTable(tableName, TEST_ROLE));
  }
}
