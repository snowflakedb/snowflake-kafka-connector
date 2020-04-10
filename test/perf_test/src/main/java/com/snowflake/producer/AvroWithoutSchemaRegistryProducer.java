package com.snowflake.producer;

import com.snowflake.Utils;
import com.snowflake.test.Enums;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.Scanner;

public class AvroWithoutSchemaRegistryProducer extends Producer<byte[]>
{
  private final KafkaProducer<String, byte[]> producer;

  public AvroWithoutSchemaRegistryProducer()
  {
    Properties props = getProperties(ByteArraySerializer.class.getCanonicalName());
    this.producer = new KafkaProducer<>(props);
  }

  @Override
  protected KafkaProducer<String, byte[]> getProducer()
  {
    return producer;
  }

  @Override
  public void send(final Enums.TestCases testCase)
  {
    System.out.println("loading table: " + testCase.getTableName() +
      " in format: " + testCase.getFormatName() + " to Kafka");
    try
    {
      Scanner scanner = getFileScanner(testCase);
      Schema schema = testCase.getTable().getSchema();
      while (scanner.hasNextLine())
      {
        GenericData.Record record = new GenericData.Record(schema);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
        fileWriter.create(schema, output);

        JsonNode data = Utils.MAPPER.readTree(scanner.nextLine());
        switch (testCase.getTable())
        {
          case ONE_G_TABLE:
            record.put("C_CUSTKEY", data.get("C_CUSTKEY").asLong());
            record.put("C_NAME", data.get("C_NAME").asText());
            record.put("C_ADDRESS", data.get("C_ADDRESS").asText());
            record.put("C_PHONE", data.get("C_PHONE").asText());
            record.put("C_ACCTBAL", data.get("C_ACCTBAL").asDouble());
            record.put("C_MKTSEGMENT", data.get("C_MKTSEGMENT").asText());
            record.put("C_COMMENT", data.get("C_COMMENT").asText());
            record.put("C_NATIONKEY", data.get("C_NATIONKEY").asLong());
            break;
          case THREE_HUNDRED_COLUMN_TABLE:
            for (int i = 0; i < 300; i++)
            {
              switch (i % 8)
              {
                case 0:
                  record.put("C" + i, data.get("C" + i).asDouble());
                  break;
                case 2:
                  record.put("C" + i, data.get("C" + i).asInt());
                  break;
                case 4:
                  record.put("C" + i, data.get("C" + i).asLong());
                  break;
                case 6:
                  record.put("C" + i, data.get("C" + i).asBoolean());
                  break;
                default:
                  record.put("C" + i, data.get("C" + i).asText());
              }
            }
        }

        fileWriter.append(record);
        fileWriter.flush();
        fileWriter.close();
        send(Utils.TEST_TOPIC, output.toByteArray());
      }
      scanner.close();
      close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("finished loading");

  }

}
