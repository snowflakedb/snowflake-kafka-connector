package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class SnowflakeRecordContent
{

  private static ObjectMapper MAPPER = new ObjectMapper();
  public static int NON_AVRO_SCHEMA = -1;
  private final JsonNode[] content;
  private final byte[] brokenData;
  private int schemaID;
  private boolean isBroken;

  /**
   * constructor for null value
   */
  public SnowflakeRecordContent()
  {
    content = new JsonNode[1];
    content[0] = MAPPER.createObjectNode();
    brokenData = null;
  }

  /**
   * constructor for native json converter
   * @param schema schema of the object
   * @param data object produced by native avro/json converters
   */
  public SnowflakeRecordContent(Schema schema, Object data)
  {
    this.content = new JsonNode[1];
    this.schemaID = NON_AVRO_SCHEMA;
    this.content[0] = RecordService.convertToJson(schema, data);
    this.isBroken = false;
    this.brokenData = null;
  }

  /**
   * constructor for json converter
   * @param data json node
   */
  SnowflakeRecordContent(JsonNode data)
  {
    this.content = new JsonNode[1];
    this.content[0] = data;
    this.isBroken = false;
    this.schemaID = NON_AVRO_SCHEMA;
    this.brokenData = null;
  }

  /**
   * constructor for avro converter without schema registry
   * @param data json node array
   */
  SnowflakeRecordContent(JsonNode[] data)
  {
    this.content = data;
    this.isBroken = false;
    this.schemaID = NON_AVRO_SCHEMA;
    this.brokenData = null;
  }

  /**
   * constructor for broken record
   * @param data broken record
   */
  public SnowflakeRecordContent(byte[] data)
  {
    this.brokenData = data;
    this.isBroken = true;
    this.schemaID = NON_AVRO_SCHEMA;
    this.content = null;
  }

  /**
   * constructor for avro converter
   * @param data json node
   * @param schemaID schema id
   */
  SnowflakeRecordContent(JsonNode data, int schemaID)
  {
    this(data);
    this.schemaID = schemaID;
  }

  /**
   *
   * @return true is record is broken
   */
  public boolean isBroken()
  {
    return this.isBroken;
  }

  /**
   *
   * @return bytes array represents broken data
   */
  public byte[] getBrokenData()
  {
    if(!isBroken)
    {
      throw SnowflakeErrors.ERROR_5011.getException();
    }
    assert this.brokenData != null;
    return this.brokenData.clone();
  }

  /**
   * @return schema id, -1 if not available
   */
  int getSchemaID()
  {
    return schemaID;
  }

  public JsonNode[] getData()
  {
    if(isBroken)
    {
      throw SnowflakeErrors.ERROR_5012.getException();
    }
    assert content != null;
    return content.clone();
  }
}
