package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;

/** Body of the SSv2 {@code :evolve-schema} pipe endpoint. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class EvolveSchemaRequest {

  /** Schema type for the JSON form of a Kafka Connect schema, as written by JsonConverter. */
  public static final String SCHEMA_TYPE_KAFKA_CONNECT = "KAFKA_CONNECT";

  private final String schemaType;
  private final JsonNode schema;
  private final String channelName;
  private final String offsetToken;
  private final Boolean normalizeColumnNames;

  /**
   * @param schemaType {@link #SCHEMA_TYPE_KAFKA_CONNECT} or {@code AVRO}
   * @param schema the schema to evolve the table to
   * @param channelName channel that saw the schema; GS returns 404 if it doesn't exist. May be null
   * @param offsetToken offset of the record that carried the schema, recorded on the
   *     schema-evolution record. May be null
   * @param normalizeColumnNames whether GS normalizes field names into column names. Null uses the
   *     server default (true)
   */
  public EvolveSchemaRequest(
      String schemaType,
      JsonNode schema,
      String channelName,
      String offsetToken,
      Boolean normalizeColumnNames) {
    this.schemaType = Objects.requireNonNull(schemaType, "schemaType");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.channelName = channelName;
    this.offsetToken = offsetToken;
    this.normalizeColumnNames = normalizeColumnNames;
  }

  @JsonProperty("schema_type")
  public String getSchemaType() {
    return schemaType;
  }

  @JsonProperty("schema")
  public JsonNode getSchema() {
    return schema;
  }

  @JsonProperty("channel_name")
  public String getChannelName() {
    return channelName;
  }

  @JsonProperty("offset_token")
  public String getOffsetToken() {
    return offsetToken;
  }

  @JsonProperty("normalize_column_names")
  public Boolean getNormalizeColumnNames() {
    return normalizeColumnNames;
  }
}
