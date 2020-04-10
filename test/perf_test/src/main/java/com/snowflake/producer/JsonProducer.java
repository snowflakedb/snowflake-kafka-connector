package com.snowflake.producer;

import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

import com.snowflake.Utils;
import com.snowflake.test.Enums.TestCases;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

public class JsonProducer extends Producer<String>
{

  private final KafkaProducer<String, String> producer;

  public JsonProducer()
  {
    Properties props = getProperties(StringSerializer.class.getCanonicalName());
    this.producer = new KafkaProducer<>(props);
  }

  @Override
  protected KafkaProducer<String, String> getProducer()
  {
    return producer;
  }

  @Override
  public void send(TestCases testCase)
  {
    System.out.println("loading table: " + testCase.getTableName() +
      " in format: " + testCase.getFormatName() + " to Kafka");
    try
    {
      Scanner scanner = getFileScanner(testCase);
      while (scanner.hasNextLine())
      {
        send(Utils.TEST_TOPIC, scanner.nextLine());
      }
      scanner.close();
      close();
    } catch (FileNotFoundException e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("finished loading");
  }
}