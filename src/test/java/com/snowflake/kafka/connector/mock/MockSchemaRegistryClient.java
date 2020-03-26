package com.snowflake.kafka.connector.mock;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MockSchemaRegistryClient implements SchemaRegistryClient
{

  private final Schema schema;
  private final byte[] data;

  public MockSchemaRegistryClient() throws IOException
  {
    String schemaJson = "{\"name\":\"test_avro\",\"type\":\"record\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}]}";
    Schema.Parser parser = new Schema.Parser();
    this.schema = parser.parse(schemaJson);

    this.data = this.serializeJson("{\"int\":1234}");
  }

  public byte[] getData()
  {
    return data.clone();
  }

  public byte[] serializeJson(String json) throws IOException {
    byte [] avroData = jsonToAvro(json, this.schema);

    // https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
    byte[] dat = new byte[5 + avroData.length];
    dat[0] = 0; // Magic byte
    dat[4] = 1; // Schema ID

    System.arraycopy(avroData, 0, dat, 5, avroData.length);


    return dat;
  }

  private static byte[] jsonToAvro(String json, Schema schema) throws
    IOException
  {
    InputStream input = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    GenericDatumWriter<GenericRecord> writer;
    Encoder encoder;
    ByteArrayOutputStream output;

    try
    {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      output = new ByteArrayOutputStream();
      writer = new GenericDatumWriter<>(schema);
      DataInputStream din = new DataInputStream(input);
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
      encoder = EncoderFactory.get().binaryEncoder(output, null);
      GenericRecord datum;
      while (true)
      {
        try
        {
          datum = reader.read(null, decoder);
        }
        catch (EOFException eof)
        {
          break;
        }
        writer.write(datum, encoder);
      }
      encoder.flush();
      return output.toByteArray();
    }
    finally
    {
      try
      {
        input.close();
      } catch (IOException e)
      {
        e.printStackTrace();
      }
    }
  }

  public byte[] getTestData()
  {
    return data.clone();
  }

  @Override
  public int register(final String s, final Schema schema) throws IOException, RestClientException
  {
    return 0;
  }

  @Override
  public int register(final String s, final Schema schema, final int i,
                      final int i1) throws IOException, RestClientException
  {
    return 0;
  }

  @Override
  public Schema getByID(final int i) throws IOException, RestClientException
  {
    return this.schema;
  }

  @Override
  public Schema getById(final int i) throws IOException, RestClientException
  {
    return this.schema;
  }

  @Override
  public Schema getBySubjectAndID(final String s, final int i) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public Schema getBySubjectAndId(final String s, final int i) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(final String s) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public SchemaMetadata getSchemaMetadata(final String s, final int i) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public int getVersion(final String s, final Schema schema) throws IOException, RestClientException
  {
    return 0;
  }

  @Override
  public List<Integer> getAllVersions(final String s) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public boolean testCompatibility(final String s, final Schema schema) throws IOException, RestClientException
  {
    return false;
  }

  @Override
  public String updateCompatibility(final String s, final String s1) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public String getCompatibility(final String s) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public String setMode(final String s) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public String setMode(final String s, final String s1) throws IOException,
    RestClientException
  {
    return null;
  }

  @Override
  public String getMode() throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public String getMode(final String s) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException,
    RestClientException
  {
    return null;
  }

  @Override
  public int getId(final String s, final Schema schema) throws IOException,
    RestClientException
  {
    return 0;
  }

  @Override
  public List<Integer> deleteSubject(final String s) throws IOException,
    RestClientException
  {
    return null;
  }

  @Override
  public List<Integer> deleteSubject(final Map<String, String> map,
                                     final String s) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public Integer deleteSchemaVersion(final String s, final String s1) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public Integer deleteSchemaVersion(final Map<String, String> map, final String s, final String s1) throws IOException, RestClientException
  {
    return null;
  }

  @Override
  public void reset()
  {

  }
}
