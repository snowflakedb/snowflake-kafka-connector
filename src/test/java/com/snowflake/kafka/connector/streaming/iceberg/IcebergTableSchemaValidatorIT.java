package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @Nested
  class SchemaEvolutionEnabled {
    public static final boolean SCHEMA_EVOLUTION = true;

    @Test
    public void shouldValidateExpectedIcebergTableSchema() {
      // given
      createIcebergTable(tableName);
      enableSchemaEvolution(tableName);

      // when, then
      schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION);
    }

    @Test
    public void shouldNotThrowExceptionWhenColumnRecordMetadataDoesNotExist() {
      // given
      createIcebergTableWithColumnClause(tableName, "some_column VARCHAR");
      enableSchemaEvolution(tableName);

      // expect
      schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION);
    }

    @Test
    public void shouldThrowExceptionWhenRecordMetadataHasInvalidType() {
      // given
      createIcebergTableWithColumnClause(tableName, "record_metadata MAP(VARCHAR, VARCHAR)");
      enableSchemaEvolution(tableName);

      // expect
      Assertions.assertThrows(
          SnowflakeKafkaConnectorException.class,
          () -> schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION));
    }
  }

  @Nested
  class SchemaEvolutionNotEnabled {
    public static final boolean SCHEMA_EVOLUTION = false;

    @Test
    public void shouldValidateExpectedIcebergTableSchema() {
      // given
      createIcebergTableNoSchemaEvolution(tableName);

      // when, then
      schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION);
    }

    @Test
    public void shouldThrowExceptionWhenTableDoesNotExist() {
      Assertions.assertThrows(
          SnowflakeKafkaConnectorException.class,
          () -> schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION));
    }

    @Test
    public void shouldThrowExceptionWhenRecordContentDoesNotExist() {
      // given
      createIcebergTableWithColumnClause(tableName, "some_column VARCHAR");

      // expect
      Assertions.assertThrows(
          SnowflakeKafkaConnectorException.class,
          () -> schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION));
    }

    @Test
    public void shouldThrowExceptionWhenRecordContentHasInvalidType() {
      // given
      createIcebergTableWithColumnClause(tableName, "record_content MAP(VARCHAR, VARCHAR)");

      // expect
      Assertions.assertThrows(
          SnowflakeKafkaConnectorException.class,
          () -> schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION));
    }

    @Test
    public void shouldNotThrowExceptionWhenColumnRecordMetadataDoesNotExist() {
      // given
      createIcebergTableWithColumnClause(tableName, "record_content object()");

      // expect
      schemaValidator.validateTable(tableName, TEST_ROLE, SCHEMA_EVOLUTION);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldThrowExceptionWhenTableDoesNotExist(boolean schemaEvolution) {
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> schemaValidator.validateTable(tableName, TEST_ROLE, schemaEvolution));
  }
}
