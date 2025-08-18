package com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.FetchSchemaClient;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchema;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeTableSchemaResolverTest {

  private final SnowflakeTableSchemaResolver schemaResolver = new SnowflakeTableSchemaResolver();
  private FetchSchemaClient originalInstance;
  private FetchSchemaClient mockInstance;

  @BeforeEach
  public void setUp() throws Exception {
    // Store the original instance
    Field instanceField = FetchSchemaClient.class.getDeclaredField("instance");
    instanceField.setAccessible(true);
    originalInstance = (FetchSchemaClient) instanceField.get(null);
    
    // Create a mock instance
    mockInstance = mock(FetchSchemaClient.class);
    
    // Replace the singleton instance with our mock
    instanceField.set(null, mockInstance);
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Restore the original instance
    Field instanceField = FetchSchemaClient.class.getDeclaredField("instance");
    instanceField.setAccessible(true);
    instanceField.set(null, originalInstance);
  }

  @Test
  public void testGetColumnTypesWithoutSchema() throws JsonProcessingException {
    String columnName = "test";
    String nonExistingColumnName = "random";
    ObjectMapper mapper = new ObjectMapper();
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, String> jsonMap = new HashMap<>();
    jsonMap.put(columnName, "value");
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("topic", mapper.writeValueAsBytes(jsonMap));
    SinkRecord recordWithoutSchema =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);

    String processedColumnName = Utils.quoteNameIfNeeded(columnName);
    String processedNonExistingColumnName = Utils.quoteNameIfNeeded(nonExistingColumnName);
    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromRecord(
            recordWithoutSchema, Collections.singletonList(processedColumnName));

    assertThat(tableSchema.getColumnInfos())
        .containsExactlyInAnyOrderEntriesOf(
            Collections.singletonMap(processedColumnName, new ColumnInfos("VARCHAR", null)));
    // Get non-existing column name should return nothing
    tableSchema =
        schemaResolver.resolveTableSchemaFromRecord(
            recordWithoutSchema, Collections.singletonList(processedNonExistingColumnName));
    assertThat(tableSchema.getColumnInfos()).isEmpty();
  }

  @Test
  public void testGetColumnTypesWithSchema() {
    JsonConverter converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaAndValue =
        converter.toConnectData(
            "topic", TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8));
    String columnName1 = Utils.quoteNameIfNeeded("regionid");
    String columnName2 = Utils.quoteNameIfNeeded("gender");
    SinkRecord recordWithoutSchema =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);

    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromRecord(
            recordWithoutSchema, Arrays.asList(columnName1, columnName2));

    assertThat(tableSchema.getColumnInfos().get(columnName1).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName1).getComments()).isEqualTo("doc");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getComments()).isNull();
  }

  @Test
  public void testLucernaTypesMapping_ZonedTimestamp() {
    // Create Avro schema with ZonedTimestamp fields, including nullable union types
    String avroSchemaJson = "{"
        + "\"type\": \"record\","
        + "\"name\": \"Value\","
        + "\"fields\": ["
        + "  {\"name\": \"last_login\", \"type\": [\"null\", {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}]},"
        + "  {\"name\": \"date_joined\", \"type\": {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}},"
        + "  {\"name\": \"regular_field\", \"type\": \"string\"}"
        + "]}";
    
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaJson);
    when(mockInstance.getSchema("test-topic-value")).thenReturn(avroSchema);

    // Create Connect schema and record
    Schema connectSchema = SchemaBuilder.struct()
        .name("Value")
        .field("last_login", Schema.OPTIONAL_STRING_SCHEMA)
        .field("date_joined", Schema.STRING_SCHEMA)
        .field("regular_field", Schema.STRING_SCHEMA)
        .build();

    Struct connectStruct = new Struct(connectSchema)
        .put("last_login", "2023-01-01T10:00:00Z")
        .put("date_joined", "2023-01-01T08:00:00Z")
        .put("regular_field", "test");

    SinkRecord record = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        connectSchema,
        connectStruct,
        0,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    );

    TableSchema tableSchema = schemaResolver.resolveTableSchemaFromRecord(
        record, Arrays.asList("last_login", "date_joined", "regular_field"));

    // Both nullable and non-nullable ZonedTimestamp fields should map to TIMESTAMP_TZ
    // This tests the fix for union types where connect.name is on the non-null type
    assertThat(tableSchema.getColumnInfos().get("last_login").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    assertThat(tableSchema.getColumnInfos().get("date_joined").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    
    // Fields without LUCERNA_TYPES should fall back to VARCHAR
    assertThat(tableSchema.getColumnInfos().get("regular_field").getColumnType()).isEqualTo("VARCHAR");
  }

  @Test
  public void testLucernaTypesMapping_AllTimestampTypes() {
    // Test all timestamp-related types in LUCERNA_TYPES
    String avroSchemaJson = "{"
        + "\"type\": \"record\","
        + "\"name\": \"TestRecord\","
        + "\"fields\": ["
        + "  {\"name\": \"zoned_timestamp\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}},"
        + "  {\"name\": \"zoned_time\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.time.ZonedTime\"}},"
        + "  {\"name\": \"date_field\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.time.Date\"}},"
        + "  {\"name\": \"time_field\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.time.Time\"}},"
        + "  {\"name\": \"micro_time\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.time.MicroTime\"}},"
        + "  {\"name\": \"timestamp\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.time.Timestamp\"}},"
        + "  {\"name\": \"micro_timestamp\", \"type\": {\"type\": \"string\", \"connect.name\": \"io.debezium.time.MicroTimestamp\"}}"
        + "]}";
    
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaJson);
    when(mockInstance.getSchema("test-topic-value")).thenReturn(avroSchema);

    // Create Connect schema and record
    Schema connectSchema = SchemaBuilder.struct()
        .name("TestRecord")
        .field("zoned_timestamp", Schema.STRING_SCHEMA)
        .field("zoned_time", Schema.STRING_SCHEMA)
        .field("date_field", Schema.STRING_SCHEMA)
        .field("time_field", Schema.STRING_SCHEMA)
        .field("micro_time", Schema.STRING_SCHEMA)
        .field("timestamp", Schema.STRING_SCHEMA)
        .field("micro_timestamp", Schema.STRING_SCHEMA)
        .build();

    Struct connectStruct = new Struct(connectSchema)
        .put("zoned_timestamp", "2023-01-01T10:00:00Z")
        .put("zoned_time", "10:00:00Z")
        .put("date_field", "2023-01-01")
        .put("time_field", "10:00:00")
        .put("micro_time", "10:00:00.123456")
        .put("timestamp", "2023-01-01T10:00:00")
        .put("micro_timestamp", "2023-01-01T10:00:00.123456");

    SinkRecord record = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        connectSchema,
        connectStruct,
        0,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    );

    TableSchema tableSchema = schemaResolver.resolveTableSchemaFromRecord(
        record, Arrays.asList("zoned_timestamp", "zoned_time", "date_field", "time_field", 
                             "micro_time", "timestamp", "micro_timestamp"));

    // Verify LUCERNA_TYPES mappings
    assertThat(tableSchema.getColumnInfos().get("zoned_timestamp").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    assertThat(tableSchema.getColumnInfos().get("zoned_time").getColumnType()).isEqualTo("TIME");
    assertThat(tableSchema.getColumnInfos().get("date_field").getColumnType()).isEqualTo("DATE");
    assertThat(tableSchema.getColumnInfos().get("time_field").getColumnType()).isEqualTo("TIME(3)");
    assertThat(tableSchema.getColumnInfos().get("micro_time").getColumnType()).isEqualTo("TIME(6)");
    assertThat(tableSchema.getColumnInfos().get("timestamp").getColumnType()).isEqualTo("TIMESTAMP_NTZ(3)");
    assertThat(tableSchema.getColumnInfos().get("micro_timestamp").getColumnType()).isEqualTo("TIMESTAMP_NTZ(6)");
  }

  @Test
  public void testComplexDebeziumSchema() {
    // Test with the actual complex Debezium schema provided by the user
    String avroSchemaJson = "{"
        + "\"type\": \"record\","
        + "\"name\": \"Value\","
        + "\"namespace\": \"leap-tenant-dev4.pg-cc.v4.public.accounts_user\","
        + "\"fields\": ["
        + "  {\"name\": \"tenant\", \"type\": \"string\"},"
        + "  {\"name\": \"password\", \"type\": \"string\"},"
        + "  {\"name\": \"last_login\", \"type\": [\"null\", {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}]},"
        + "  {\"name\": \"is_superuser\", \"type\": \"boolean\"},"
        + "  {\"name\": \"username\", \"type\": \"string\"},"
        + "  {\"name\": \"first_name\", \"type\": \"string\"},"
        + "  {\"name\": \"last_name\", \"type\": \"string\"},"
        + "  {\"name\": \"is_staff\", \"type\": \"boolean\"},"
        + "  {\"name\": \"is_active\", \"type\": \"boolean\"},"
        + "  {\"name\": \"date_joined\", \"type\": {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}},"
        + "  {\"name\": \"created\", \"type\": {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}},"
        + "  {\"name\": \"modified\", \"type\": {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}},"
        + "  {\"name\": \"created_by_username\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"created_by_name\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"modified_by_username\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"modified_by_name\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"removed\", \"type\": [\"null\", {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}]},"
        + "  {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.data.Uuid\"}},"
        + "  {\"name\": \"email\", \"type\": \"string\"},"
        + "  {\"name\": \"metadata\", \"type\": {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.data.Json\"}},"
        + "  {\"name\": \"performance_logging\", \"type\": \"boolean\"},"
        + "  {\"name\": \"status\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"phone_number\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"address\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"preferred_name\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"act_as_regular_user\", \"type\": \"boolean\"},"
        + "  {\"name\": \"tenant_id\", \"type\": [\"null\", {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.data.Uuid\"}]},"
        + "  {\"name\": \"permission_logging\", \"type\": \"string\"},"
        + "  {\"name\": \"country\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"date_of_birth\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"date_of_hire\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"department\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"employment_type\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"gender\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"role\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"title\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"user_type\", \"type\": [\"null\", \"string\"]}"
        + "  ]"
        + "}";
    
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaJson);
    when(mockInstance.getSchema("test-topic-value")).thenReturn(avroSchema);

    // Create Connect schema matching the Avro schema
    Schema connectSchema = SchemaBuilder.struct()
        .name("Value")
        .field("tenant", Schema.STRING_SCHEMA)
        .field("password", Schema.STRING_SCHEMA)
        .field("last_login", Schema.OPTIONAL_STRING_SCHEMA)
        .field("is_superuser", Schema.BOOLEAN_SCHEMA)
        .field("username", Schema.STRING_SCHEMA)
        .field("first_name", Schema.STRING_SCHEMA)
        .field("last_name", Schema.STRING_SCHEMA)
        .field("is_staff", Schema.BOOLEAN_SCHEMA)
        .field("is_active", Schema.BOOLEAN_SCHEMA)
        .field("date_joined", Schema.STRING_SCHEMA)
        .field("created", Schema.STRING_SCHEMA)
        .field("modified", Schema.STRING_SCHEMA)
        .field("created_by_username", Schema.OPTIONAL_STRING_SCHEMA)
        .field("created_by_name", Schema.OPTIONAL_STRING_SCHEMA)
        .field("modified_by_username", Schema.OPTIONAL_STRING_SCHEMA)
        .field("modified_by_name", Schema.OPTIONAL_STRING_SCHEMA)
        .field("removed", Schema.OPTIONAL_STRING_SCHEMA)
        .field("uuid", Schema.STRING_SCHEMA)
        .field("email", Schema.STRING_SCHEMA)
        .field("metadata", Schema.STRING_SCHEMA)
        .field("performance_logging", Schema.BOOLEAN_SCHEMA)
        .field("status", Schema.OPTIONAL_STRING_SCHEMA)
        .field("phone_number", Schema.OPTIONAL_STRING_SCHEMA)
        .field("address", Schema.OPTIONAL_STRING_SCHEMA)
        .field("preferred_name", Schema.OPTIONAL_STRING_SCHEMA)
        .field("act_as_regular_user", Schema.BOOLEAN_SCHEMA)
        .field("tenant_id", Schema.OPTIONAL_STRING_SCHEMA)
        .field("permission_logging", Schema.STRING_SCHEMA)
        .field("country", Schema.OPTIONAL_STRING_SCHEMA)
        .field("date_of_birth", Schema.OPTIONAL_STRING_SCHEMA)
        .field("date_of_hire", Schema.OPTIONAL_STRING_SCHEMA)
        .field("department", Schema.OPTIONAL_STRING_SCHEMA)
        .field("employment_type", Schema.OPTIONAL_STRING_SCHEMA)
        .field("gender", Schema.OPTIONAL_STRING_SCHEMA)
        .field("role", Schema.OPTIONAL_STRING_SCHEMA)
        .field("title", Schema.OPTIONAL_STRING_SCHEMA)
        .field("user_type", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    Struct connectStruct = new Struct(connectSchema)
        .put("tenant", "test-tenant")
        .put("password", "password123")
        .put("last_login", "2023-01-01T10:00:00Z")
        .put("is_superuser", false)
        .put("username", "testuser")
        .put("first_name", "John")
        .put("last_name", "Doe")
        .put("is_staff", true)
        .put("is_active", true)
        .put("date_joined", "2023-01-01T08:00:00Z")
        .put("created", "2023-01-01T08:00:00Z")
        .put("modified", "2023-01-01T09:00:00Z")
        .put("created_by_username", "admin")
        .put("created_by_name", "Administrator")
        .put("modified_by_username", "admin")
        .put("modified_by_name", "Administrator")
        .put("removed", null)
        .put("uuid", "123e4567-e89b-12d3-a456-426614174000")
        .put("email", "test@example.com")
        .put("metadata", "{\"key\": \"value\"}")
        .put("performance_logging", false)
        .put("status", "active")
        .put("phone_number", "555-1234")
        .put("address", "123 Main St")
        .put("preferred_name", "Johnny")
        .put("act_as_regular_user", false)
        .put("tenant_id", "456e7890-e89b-12d3-a456-426614174001")
        .put("permission_logging", "enabled")
        .put("country", "US")
        .put("date_of_birth", "1990-01-01")
        .put("date_of_hire", "2020-01-01")
        .put("department", "Engineering")
        .put("employment_type", "Full-time")
        .put("gender", "M")
        .put("role", "Developer")
        .put("title", "Software Engineer")
        .put("user_type", "employee");

    SinkRecord record = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        connectSchema,
        connectStruct,
        0,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    );

    // Test timestamp fields that should map to TIMESTAMP_TZ via LUCERNA_TYPES
    TableSchema timestampFieldsSchema = schemaResolver.resolveTableSchemaFromRecord(
        record, Arrays.asList("\"LAST_LOGIN\"", "\"DATE_JOINED\"", "\"CREATED\"", "\"MODIFIED\"", "\"REMOVED\""));

    // All ZonedTimestamp fields should map to TIMESTAMP_TZ via LUCERNA_TYPES
    assertThat(timestampFieldsSchema.getColumnInfos().get("\"LAST_LOGIN\"").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    assertThat(timestampFieldsSchema.getColumnInfos().get("\"DATE_JOINED\"").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    assertThat(timestampFieldsSchema.getColumnInfos().get("\"CREATED\"").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    assertThat(timestampFieldsSchema.getColumnInfos().get("\"MODIFIED\"").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    assertThat(timestampFieldsSchema.getColumnInfos().get("\"REMOVED\"").getColumnType()).isEqualTo("TIMESTAMP_TZ");

    // Test non-LUCERNA fields should fall back to default types
TableSchema nonLucernaSchema = schemaResolver.resolveTableSchemaFromRecord(
          record, Arrays.asList("\"TENANT\"", "\"PASSWORD\"", "\"IS_SUPERUSER\"", "\"UUID\"", "\"METADATA\"", "\"TENANT_ID\""));

      assertThat(nonLucernaSchema.getColumnInfos().get("\"TENANT\"").getColumnType()).isEqualTo("VARCHAR");
      assertThat(nonLucernaSchema.getColumnInfos().get("\"PASSWORD\"").getColumnType()).isEqualTo("VARCHAR");
      assertThat(nonLucernaSchema.getColumnInfos().get("\"IS_SUPERUSER\"").getColumnType()).isEqualTo("BOOLEAN");
      // UUID and JSON fields should fall back to VARCHAR since they're not in LUCERNA_TYPES
      assertThat(nonLucernaSchema.getColumnInfos().get("\"UUID\"").getColumnType()).isEqualTo("VARCHAR");
      assertThat(nonLucernaSchema.getColumnInfos().get("\"METADATA\"").getColumnType()).isEqualTo("VARCHAR");
      assertThat(nonLucernaSchema.getColumnInfos().get("\"TENANT_ID\"").getColumnType()).isEqualTo("VARCHAR");
  }

  @Test
  public void testLucernaTypesMapping_NullableUnionTypeBugFix() {
    // This test specifically targets the bug where nullable fields ["null", "string"] 
    // don't find the connect.name property because it's on the string schema, not the union
    String avroSchemaJson = "{"
        + "\"type\": \"record\","
        + "\"name\": \"TestRecord\","
        + "\"fields\": ["
        + "  {\"name\": \"nullable_timestamp\", \"type\": [\"null\", {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}]},"
        + "  {\"name\": \"nullable_uuid\", \"type\": [\"null\", {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.data.Uuid\"}]},"
        + "  {\"name\": \"nullable_regular\", \"type\": [\"null\", \"string\"]},"
        + "  {\"name\": \"non_nullable_timestamp\", \"type\": {\"type\": \"string\", \"connect.version\": 1, \"connect.name\": \"io.debezium.time.ZonedTimestamp\"}}"
        + "]}";
    
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaJson);
    when(mockInstance.getSchema("test-topic-value")).thenReturn(avroSchema);

    Schema connectSchema = SchemaBuilder.struct()
        .name("TestRecord")
        .field("nullable_timestamp", Schema.OPTIONAL_STRING_SCHEMA)
        .field("nullable_uuid", Schema.OPTIONAL_STRING_SCHEMA)
        .field("nullable_regular", Schema.OPTIONAL_STRING_SCHEMA)
        .field("non_nullable_timestamp", Schema.STRING_SCHEMA)
        .build();

    Struct connectStruct = new Struct(connectSchema)
        .put("nullable_timestamp", "2023-01-01T10:00:00Z")
        .put("nullable_uuid", "123e4567-e89b-12d3-a456-426614174000")
        .put("nullable_regular", "regular_value")
        .put("non_nullable_timestamp", "2023-01-01T11:00:00Z");

    SinkRecord record = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        connectSchema,
        connectStruct,
        0,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    );

    TableSchema tableSchema = schemaResolver.resolveTableSchemaFromRecord(
        record, Arrays.asList("nullable_timestamp", "nullable_uuid", "nullable_regular", "non_nullable_timestamp"));

    // The bug fix should allow nullable timestamp fields to be properly mapped to TIMESTAMP_TZ
    assertThat(tableSchema.getColumnInfos().get("nullable_timestamp").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    assertThat(tableSchema.getColumnInfos().get("non_nullable_timestamp").getColumnType()).isEqualTo("TIMESTAMP_TZ");
    
    // UUID fields should fall back to VARCHAR (not in LUCERNA_TYPES)
    assertThat(tableSchema.getColumnInfos().get("nullable_uuid").getColumnType()).isEqualTo("VARCHAR");
    
    // Regular nullable fields should fall back to VARCHAR
    assertThat(tableSchema.getColumnInfos().get("nullable_regular").getColumnType()).isEqualTo("VARCHAR");
  }

  @Test
  public void testLucernaTypesFallbackWhenSchemaUnavailable() {
    // Test fallback behavior when schema registry returns null
    when(mockInstance.getSchema("test-topic-value")).thenReturn(null);

    Schema connectSchema = SchemaBuilder.struct()
        .name("TestRecord")
        .field("timestamp_field", Schema.STRING_SCHEMA)
        .field("regular_field", Schema.STRING_SCHEMA)
        .field("boolean_field", Schema.BOOLEAN_SCHEMA)
        .build();

    Struct connectStruct = new Struct(connectSchema)
        .put("timestamp_field", "2023-01-01T10:00:00Z")
        .put("regular_field", "test")
        .put("boolean_field", true);

    SinkRecord record = new SinkRecord(
        "test-topic",
        0,
        null,
        null,
        connectSchema,
        connectStruct,
        0,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    );

    TableSchema tableSchema = schemaResolver.resolveTableSchemaFromRecord(
        record, Arrays.asList("timestamp_field", "regular_field", "boolean_field"));

    // When no Avro schema is available, should fall back to Kafka type mapping
    assertThat(tableSchema.getColumnInfos().get("timestamp_field").getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get("regular_field").getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get("boolean_field").getColumnType()).isEqualTo("BOOLEAN");
  }
}
