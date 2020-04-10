package com.snowflake.producer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

import com.snowflake.test.Enums.TestCases;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class Producer<T>
{
  private static final String URL = "localhost:9092";
  private static final String CLIENT_ID = "Kafka_test";
  private long key = 0;

  //abstract elements
  abstract protected KafkaProducer<String, T> getProducer();

  abstract public void send(TestCases testCase);

  protected boolean useSchemaRegistry()
  {
    return false;
  }

  public void send(String topic, T data)
  {
    getProducer().send(
      new ProducerRecord<>(
        topic, nextKeyString(), data
      )
    );
  }

  public void close()
  {
    getProducer().close();
  }

  private String nextKeyString()
  {
    return (key++) + "";
  }


  Properties getProperties(String valueSerializer)
  {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      org.apache.kafka.common.serialization.StringSerializer.class.getCanonicalName()
    );
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer
    );

    if(useSchemaRegistry())
    {
      props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    }
    return props;
  }

  Scanner getFileScanner(TestCases testCase) throws FileNotFoundException
  {
    String fileName = "data/" + testCase.getTableName() + ".json";
    File input = new File(fileName);
    return new Scanner(input);
  }

}