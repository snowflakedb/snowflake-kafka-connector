/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class SnowflakeAvroConverter extends SnowflakeConverter
{
  private SchemaRegistryClient schemaRegistry = null;

  public static final String BREAK_ON_SCHEMA_REGISTRY_ERROR = "break.on.schema.registry.error";
  // By default, we don't break when schema registry is not found
  private boolean breakOnSchemaRegistryError = false;

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey)
  {
    readBreakOnSchemaRegistryError(configs);
    try
    { //todo: graceful way to check schema registry
      AvroConverterConfig avroConverterConfig = new AvroConverterConfig
        (configs);
      schemaRegistry = new CachedSchemaRegistryClient(
        avroConverterConfig.getSchemaRegistryUrls(),
        avroConverterConfig.getMaxSchemasPerSubject(),
        configs
      );
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_0012.getException(e);
    }
  }

  void readBreakOnSchemaRegistryError(final Map<String, ?> configs)
  {
    try
    {
      Object shouldBreak = configs.get(BREAK_ON_SCHEMA_REGISTRY_ERROR);
      if (shouldBreak instanceof String)
      {
        breakOnSchemaRegistryError = ((String) shouldBreak).toLowerCase().equals("true");
      }
    } catch (Exception e)
    {
      // do nothing
    }
  }

  // for testing only
  boolean getBreakOnSchemaRegistryError()
  {
    return breakOnSchemaRegistryError;
  }

  /**
   * set a schema registry client for test use only
   * @param schemaRegistryClient mock schema registry client
   */
  void setSchemaRegistry(SchemaRegistryClient schemaRegistryClient)
  {
    this.schemaRegistry = schemaRegistryClient;
  }


  /**
   * cast bytes array to JsonNode array
   *
   * @param s     topic, unused
   * @param bytes input bytes array
   * @return JsonNode array
   */
  @Override
  public SchemaAndValue toConnectData(final String s, final byte[] bytes)
  {
    if(bytes == null)
    {
      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent());
    }
    ByteBuffer buffer;
    int id;
    try
    {
      buffer = ByteBuffer.wrap(bytes);
      if (buffer.get() != 0)
      {
        throw SnowflakeErrors.ERROR_0010.getException("unknown bytes");
      }
      id = buffer.getInt();
    } catch (Exception e)
    {
      return logErrorAndReturnBrokenRecord(e, bytes);
    }

    // If there is any error while getting schema from schema registry,
    // throw error and break the connector
    Schema schema;
    try
    {
      schema = schemaRegistry.getById(id);
    } catch (Exception e)
    {
      if (breakOnSchemaRegistryError)
      {
        throw SnowflakeErrors.ERROR_0011.getException(e);
      }
      else
      {
        return logErrorAndReturnBrokenRecord(e, bytes);
      }
    }

    try {
      int length = buffer.limit() - 1 - 4;
      byte[] data = new byte[length];
      buffer.get(data, 0, length);

      return new SchemaAndValue(new SnowflakeJsonSchema(),
        new SnowflakeRecordContent(parseAvroWithSchema(data, schema), id));
    } catch (Exception e)
    {
      return logErrorAndReturnBrokenRecord(e, bytes);
    }
  }

  private SchemaAndValue logErrorAndReturnBrokenRecord(final Exception e, final byte[] bytes)
  {

    LOGGER.error(Logging.logMessage("failed to parse AVRO record\n" + e.getMessage()));
    return new SchemaAndValue(new SnowflakeJsonSchema(),
      new SnowflakeRecordContent(bytes));
  }

  /**
   * Parse Avro record with schema
   *
   * @param bytes  avro data
   * @param schema avro schema
   * @return JsonNode  array
   */
  private JsonNode parseAvroWithSchema(final byte[] bytes, Schema schema)
  {
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    InputStream input = new ByteArrayInputStream(bytes);
    Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
    try
    {
      GenericRecord datum = reader.read(null, decoder);
      return mapper.readTree(datum.toString());
    } catch (IOException e)
    {
      throw SnowflakeErrors.ERROR_0010.getException("Failed to parse AVRO " +
        "record\n" + e.toString());
    }
  }

}
