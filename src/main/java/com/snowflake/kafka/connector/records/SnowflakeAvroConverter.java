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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
import java.util.ArrayList;
import java.util.Map;

public class SnowflakeAvroConverter extends SnowflakeConverter
{
  private SchemaRegistryClient schemaRegistry = null;

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey)
  {
    try { //todo: graceful way to check schema registry
      AvroConverterConfig avroConverterConfig = new AvroConverterConfig
          (configs);
      schemaRegistry = new CachedSchemaRegistryClient(
          avroConverterConfig.getSchemaRegistryUrls(),
          avroConverterConfig.getMaxSchemasPerSubject(),
          configs
      );
      LOGGER.info("Schema Loaded from Schema Registry");
    }
    catch (Exception e)
    {
      schemaRegistry = null;
      LOGGER.info("Schema Registry is disabled");
    }
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
    if(schemaRegistry == null)
    {
      return parseAvro(bytes);
    }
    else {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      if(buffer.get() != 0) throw new IllegalArgumentException("unknown bytes");
      int id = buffer.getInt();
      Schema schema;
      try
      {
        schema = schemaRegistry.getById(id);
      } catch (Exception e)
      {
        throw new IllegalArgumentException("can not access schema");
      }

      int length = buffer.limit() - 1 - 4;
      byte[] data = new byte[length];
      buffer.get(data,0,length);

      return parseAvroWithSchema(data, schema);
    }
  }

  /**
   * Parse Avro record with schema
   * @param bytes avro data
   * @param schema avro schema
   * @return JsonNode  array
   */
  private SchemaAndValue parseAvroWithSchema(final byte[] bytes, Schema schema)
  {
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    InputStream input = new ByteArrayInputStream(bytes);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    JsonEncoder encoder;
    try
    {
      encoder = EncoderFactory.get().jsonEncoder(schema,output, false);
    } catch (IOException e)
    {
      throw new IllegalArgumentException("can not create json encoder");
    }

    Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
    try
    {
      GenericRecord datum = reader.read(null, decoder);
      writer.write(datum, encoder);
      encoder.flush();
      output.flush();

      String json = new String(output.toByteArray());

      JsonNode[] result = {MAPPER.readTree(json)};
      return new SchemaAndValue(new SnowflakeJsonSchema(), result);
    } catch (IOException e)
    {
      throw new IllegalArgumentException("can not parse avro record");
    }
  }


  /**
   * Parse Avro record without schema
   * @param bytes Avro data
   * @return Json Array
   */
  private SchemaAndValue parseAvro(final byte[] bytes)
  {
    //avro input parser
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader;

    try
    {
      dataFileReader = new DataFileReader<>(new SeekableInputSource(bytes),
          datumReader);
    } catch (Exception e)
    {
      LOGGER.error("can not parse AVRO data\n {}", e.getMessage());
      throw new IllegalArgumentException("can not parse AVRO data\n" + e
          .getMessage());
    }

    ArrayList<JsonNode> buffer = new ArrayList<>();
    while (dataFileReader.hasNext())
    {
      String jsonString = dataFileReader.next().toString();
      try
      {
        buffer.add(MAPPER.readTree(jsonString));
      } catch (IOException e)
      {
        LOGGER.error("error: {} \n when parsing json record: {}", e
                .getMessage(),
            jsonString);

        throw new IllegalArgumentException("Input record is not a valid json " +
            "data\n" + jsonString);
      }
    }

    JsonNode[] result = new JsonNode[buffer.size()];
    for (int i = 0; i < buffer.size(); i++)
    {
      result[i] = buffer.get(i);
    }
    return new SchemaAndValue(new SnowflakeJsonSchema(), result);
  }
}
