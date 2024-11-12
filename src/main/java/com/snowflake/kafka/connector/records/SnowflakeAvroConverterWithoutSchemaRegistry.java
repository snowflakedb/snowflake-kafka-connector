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

import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.SchemaAndValue;

public class SnowflakeAvroConverterWithoutSchemaRegistry extends SnowflakeConverter {
  /**
   * Parse Avro record without schema
   *
   * @param topic topic name, unused
   * @param value Avro data
   * @return Json Array
   */
  @Override
  public SchemaAndValue toConnectData(final String topic, final byte[] value) {
    if (value == null) {
      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent());
    }
    try {
      // avro input parser
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> dataFileReader;

      try {
        dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(value), datumReader);
      } catch (Exception e) {
        throw SnowflakeErrors.ERROR_0010.getException(
            "Failed to parse AVRO " + "record\n" + e.getMessage());
      }

      ArrayList<JsonNode> buffer = new ArrayList<>();
      while (dataFileReader.hasNext()) {
        String jsonString = dataFileReader.next().toString();
        try {
          buffer.add(mapper.readTree(jsonString));
        } catch (IOException e) {
          throw SnowflakeErrors.ERROR_0010.getException(
              "Failed to parse JSON"
                  + " "
                  + "record\nInput String: "
                  + jsonString
                  + "\n"
                  + e.getMessage());
        }
      }

      JsonNode[] result = new JsonNode[buffer.size()];
      for (int i = 0; i < buffer.size(); i++) {
        result[i] = buffer.get(i);
      }

      try {
        dataFileReader.close();
      } catch (IOException e) {
        throw SnowflakeErrors.ERROR_0010.getException(
            "Failed to parse AVRO " + "record\n" + e.getMessage());
      }

      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent(result));
    } catch (Exception e) {
      LOGGER.error("Failed to parse AVRO record\n" + e.getMessage());
      return new SchemaAndValue(new SnowflakeJsonSchema(), new SnowflakeRecordContent(value));
    }
  }
}
