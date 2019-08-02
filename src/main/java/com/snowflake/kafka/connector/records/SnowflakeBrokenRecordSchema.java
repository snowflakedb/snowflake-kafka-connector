package com.snowflake.kafka.connector.records;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Map;

public class SnowflakeBrokenRecordSchema implements Schema
{
  public static String NAME = "SNOWFLAKE_BROKEN_RECORD_SCHEMA";
  static int VERSION = 1;
  @Override
  public Type type()
  {
    return Type.BYTES;
  }

  @Override
  public boolean isOptional()
  {
    return false;
  }

  @Override
  public Object defaultValue()
  {
    return null;
  }

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public Integer version()
  {
    return VERSION;
  }

  @Override
  public String doc()
  {
    return null;
  }

  @Override
  public Map<String, String> parameters()
  {
    return null;
  }

  @Override
  public Schema keySchema()
  {
    return null;
  }

  @Override
  public Schema valueSchema()
  {
    return null;
  }

  @Override
  public List<Field> fields()
  {
    return null;
  }

  @Override
  public Field field(final String s)
  {
    return null;
  }

  @Override
  public Schema schema()
  {
    return null;
  }
}
