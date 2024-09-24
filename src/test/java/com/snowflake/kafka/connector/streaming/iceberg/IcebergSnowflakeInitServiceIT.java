package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IcebergSnowflakeInitServiceIT extends BaseIcebergIT {

    private static IcebergSnowflakeInitService icebergSnowflakeInitService;

    private String tableName;

    @BeforeAll
    // overrides the base class @BeforeAll
    public static void setup() {
        conn = TestUtils.getConnectionServiceForStreaming();
        icebergSnowflakeInitService = new IcebergSnowflakeInitService(conn);
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
        icebergSnowflakeInitService.initializeIcebergTableProperties(tableName);

        // then
        assertThat(describeRecordMetadataType(tableName)).isEqualTo("OBJECT(offset NUMBER(10,0), " +
                "topic VARCHAR(16777216), " +
                "partition NUMBER(10,0), " +
                "key VARCHAR(16777216), " +
                "schema_id NUMBER(10,0), " +
                "key_schema_id NUMBER(10,0), " +
                "CreateTime NUMBER(19,0), " +
                "SnowflakeConnectorPushTime NUMBER(19,0), " +
                "headers MAP(VARCHAR(16777216), " +
                "VARCHAR(16777216)))");
    }

    @Test
    void shouldThrowExceptionWhenTableDoesNotExist() {
        assertThatThrownBy(() -> icebergSnowflakeInitService.initializeIcebergTableProperties(tableName))
                .isInstanceOf(SnowflakeKafkaConnectorException.class);
    }

    @Test
    void shouldThrowExceptionWhenRecordMetadataDoesNotExist() {
        // given
        createIcebergTableWithColumnClause(tableName, "some_column VARCHAR");

        // expect
        assertThatThrownBy(() -> icebergSnowflakeInitService.initializeIcebergTableProperties(tableName))
                .isInstanceOf(SnowflakeKafkaConnectorException.class);
    }
}
