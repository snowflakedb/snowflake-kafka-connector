package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import org.apache.kafka.connect.data.Schema;

public class SnowflakeRecordContent {

  private static ObjectMapper MAPPER = new ObjectMapper();
  public static int NON_AVRO_SCHEMA = -1;
  private final JsonNode[] content;
  private final byte[] brokenData;
  private int schemaID;
  private boolean isBroken;

  // We have to introduce this field so as to distinguish a null value record from a record whose
  // actual contents are an empty json node.
  // This is only set inside a constructor which is called when a byte value found in the record is
  // null.
  private boolean isNullValueRecord;

  /**
   * Constructor for null value.
   *
   * <p>If we change this logic in future, we need to carefully modify how we handle tombstone
   * records.
   *
   * <p>@see SnowflakeSinkServiceV1#shouldSkipNullValue(SinkRecord)
   */
  public SnowflakeRecordContent() {
    content = new JsonNode[1];
    content[0] = MAPPER.createObjectNode();
    brokenData = null;
    isNullValueRecord = true;
  }

  /**
   * constructor for native json converter
   *
   * @param schema schema of the object
   * @param data object produced by native avro/json converters
   * @param isStreaming indicates whether this is part of snowpipe streaming
   */
  public SnowflakeRecordContent(Schema schema, Object data, boolean isStreaming) {
    this.content = new JsonNode[1];
    this.schemaID = NON_AVRO_SCHEMA;
    this.content[0] = RecordService.convertToJson(schema, data, isStreaming);
    this.isBroken = false;
    this.brokenData = null;
  }

  /**
   * constructor for json converter
   *
   * @param data json node
   */
  public SnowflakeRecordContent(JsonNode data) {
    this.content = new JsonNode[1];
    this.content[0] = data;
    this.isBroken = false;
    this.schemaID = NON_AVRO_SCHEMA;
    this.brokenData = null;
  }

  /**
   * constructor for avro converter without schema registry
   *
   * @param data json node array
   */
  SnowflakeRecordContent(JsonNode[] data) {
    this.content = data;
    this.isBroken = false;
    this.schemaID = NON_AVRO_SCHEMA;
    this.brokenData = null;
  }

  /**
   * constructor for broken record
   *
   * @param data broken record
   */
  public SnowflakeRecordContent(byte[] data) {
    this.brokenData = data;
    this.isBroken = true;
    this.schemaID = NON_AVRO_SCHEMA;
    this.content = null;
  }

  /**
   * constructor for avro converter
   *
   * @param data json node
   * @param schemaID schema id
   */
  SnowflakeRecordContent(JsonNode data, int schemaID) {
    this(data);
    this.schemaID = schemaID;
  }

  /** @return true is record is broken */
  public boolean isBroken() {
    return this.isBroken;
  }

  /** @return bytes array represents broken data */
  public byte[] getBrokenData() {
    if (!isBroken) {
      throw SnowflakeErrors.ERROR_5011.getException();
    }
    assert this.brokenData != null;
    return this.brokenData.clone();
  }

  /** @return schema id, -1 if not available */
  int getSchemaID() {
    return schemaID;
  }

  public JsonNode[] getData() {
    if (isBroken) {
      throw SnowflakeErrors.ERROR_5012.getException();
    }
    assert content != null;
    return content.clone();
  }

  /**
   * Check if primary reason for this record content's value to be an empty json String, a null
   * value?
   *
   * <p>i.e if value passed in by record is empty json node (`{}`), we don't interpret this as null
   * value.
   *
   * @return true if content value is empty json node as well as isNullValueRecord is set to true.
   */
  public boolean isRecordContentValueNull() {
    if (content != null && content[0].isEmpty() && isNullValueRecord) {
      return true;
    }
    return false;
  }
}
