package com.snowflake.kafka.connector.internal.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.InjectAssertDbConnection;
import com.snowflake.kafka.connector.InjectAssertDbConnectionExtension;
import com.snowflake.kafka.connector.InjectQueryRunner;
import com.snowflake.kafka.connector.InjectQueryRunnerExtension;
import com.snowflake.kafka.connector.InjectSnowflakeDataSourceExtension;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.db.type.AssertDbConnection;
import org.assertj.db.type.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_V2_ENABLED;
import static com.snowflake.kafka.connector.internal.TestUtils.assertWithRetry;
import static com.snowflake.kafka.connector.internal.TestUtils.tableSize;
import static java.lang.String.format;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.db.api.Assertions.assertThat;

@ExtendWith({InjectSnowflakeDataSourceExtension.class,
    InjectQueryRunnerExtension.class, InjectAssertDbConnectionExtension.class})
class UserDefinedPipeAndTableIT {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
    private final Map<String, ?> testPayload = Map.of(
        "city", "Pcim Górny",
        "age", 30,
        "married", false
    );
    private String table;
    private String topic;
    private Map<String, String> config;
    private String pipe;
    private TopicPartition topicPartition;

    @InjectQueryRunner
    private QueryRunner queryRunner;

    @InjectAssertDbConnection
    private AssertDbConnection assertDb;

    @BeforeEach
    void setup() throws SQLException {
        config = TestUtils.getConfForStreaming();
        // Enable user defined table and pipe
        config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
        config.put(SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS, "true");
        table = TestUtils.randomTableName();
        topic = table;
        pipe = PipeNameProvider.pipeName(config, table);
        topicPartition = new TopicPartition(topic, 0);


        queryRunner.execute(format("create table %s (city varchar, age number, married boolean)",table));
        queryRunner.execute(format("CREATE OR REPLACE PIPE %s " +
            "AS COPY INTO %s FROM (SELECT $1:RECORD_CONTENT.city, $1:RECORD_CONTENT.age,  $1:RECORD_CONTENT.married FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))", pipe, table));
    }


    @AfterEach
    void afterEach() {
        TestUtils.dropTable(table);
        TestUtils.dropPipe(pipe);
    }


    @Test
    public void test_streaming_ingestion_with_user_defined_table_and_pipe() throws Exception {
        SnowflakeSinkService service = StreamingSinkServiceBuilder.builder(conn, config).withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition))).build();
        List<SinkRecord> records = getRecordsToInsert();

        service.startPartition(table, topicPartition);
        service.insert(records);

        // Wait for data to be ingested into the interactive table
        assertWithRetry(() -> tableSize(table) == 2);
        service.closeAll();
        // Create a Table object for the "person" table
        Table destinationTable = assertDb.table(table).build();

        // Assert that the table has exactly 2 rows with the given values
        assertThat(destinationTable)
            .hasNumberOfRows(2)
            .row(0)
            .value("city").isEqualTo("Pcim Górny")
            .value("age").isEqualTo(30)
            .value("married").isEqualTo(false);

    }

    private List<SinkRecord> getRecordsToInsert() throws JsonProcessingException {
        SnowflakeConverter converter = new SnowflakeJsonConverter();
        SchemaAndValue input = converter.toConnectData(topic, jsonPayload());
        return List.of(
            new SinkRecord(topic, 0, STRING_SCHEMA, "test_key1", input.schema(), input.value(), 1),
            new SinkRecord(topic, 0, STRING_SCHEMA, "test_key2", input.schema(), input.value(), 2)
        );
    }

    private byte[] jsonPayload() throws JsonProcessingException {
        return objectMapper.writeValueAsString(testPayload).getBytes(StandardCharsets.UTF_8);
    }

}
