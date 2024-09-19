package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergTableSchemaValidatorIT {

    private static final String TEST_ROLE = "testrole_kafka"

    private static final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
    private static final IcebergTableSchemaValidator schemaValidator = new IcebergTableSchemaValidator(conn);

    private String tableName;

    @BeforeEach
    public void setUp() {
        tableName = TestUtils.randomTableName();
    }

    @AfterEach
    public void tearDown() {
        TestUtils.dropIcebergTable(tableName);
    }
    
    @Test
    public void shouldValidateExpectedIcebergTableSchema() throws Exception {
        // given
        TestUtils.createIcebergTable(tableName);
        TestUtils.enableSchemaEvolution(tableName);

        // when, then
        schemaValidator.validateTable(tableName, TEST_ROLE);
    }

    @Test
    public void shouldThrowExceptionWhenTableDoesNotExist() {
        // Assertions.assertThrows(RuntimeException.class, () -> schemaValidator.validateTable(tableName, TEST_ROLE));
    }

    @Test
    public void shouldThrowExceptionWhenRecordMetadataDoesNotExist() {}

    @Test
    public void shouldThrowExceptionWhenRecordMetadataHasInvalidType() {}
}
