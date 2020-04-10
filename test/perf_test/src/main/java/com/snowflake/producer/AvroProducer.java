package com.snowflake.producer;

import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;


public abstract class AvroProducer<T> extends Producer<T>
{

  private final Properties props;
  private final KafkaProducer<String, T> producer;

  AvroProducer()
  {
    this.props = getProperties(KafkaAvroSerializer.class.getCanonicalName());
    this.producer = new KafkaProducer<>(props);
  }

  @Override
  protected KafkaProducer<String, T> getProducer()
  {
    return producer;
  }

  @Override
  protected boolean useSchemaRegistry()
  {
    return true;
  }

}