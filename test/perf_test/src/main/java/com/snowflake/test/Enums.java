package com.snowflake.test;

import com.snowflake.Utils;
import org.apache.avro.Schema;

public class Enums
{
  public enum TestCases
  {
    JSON_ONE_G_TABLE(
      Tables.ONE_G_TABLE,
      Formats.JSON
    ),
    JSON_THREE_HUNDRED_COLUMN_TABLE(
      Tables.THREE_HUNDRED_COLUMN_TABLE,
      Formats.JSON
    ),
    AVRO_WITH_SCHEMA_REGISTRY_ONE_G_TABLE(
      Tables.ONE_G_TABLE,
      Formats.AVRO_WITH_SCHEMA_REGISTRY
    ),
    AVRO_WITH_SCHEMA_REGISTRY_THREE_HUNDRED_COLUMN_TABLE(
      Tables.THREE_HUNDRED_COLUMN_TABLE,
      Formats.AVRO_WITH_SCHEMA_REGISTRY
    ),
    AVRO_WITHOUT_SCHEMA_REGISTRY_ONE_G_TABLE(
      Tables.ONE_G_TABLE,
      Formats.AVRO_WITHOUT_SCHEMA_REGISTRY
    ),
    AVRO_WITHOUT_SCHEMA_REGISTRY_THREE_HUNDRED_COLUMN_TABLE(
      Tables.THREE_HUNDRED_COLUMN_TABLE,
      Formats.AVRO_WITHOUT_SCHEMA_REGISTRY
    );


    private final Tables table;
    private final Formats format;

    TestCases(Tables table, Formats format)
    {
      this.table = table;
      this.format = format;
    }

    public Tables getTable()
    {
      return this.table;
    }

    public Formats getFormat()
    {
      return this.format;
    }

    public String getTableName()
    {
      return table.toString();
    }

    public String getFormatName()
    {
      return format.toString();
    }

    @Override
    public String toString()
    {
      return "load " + table.toString() + " in " + format.toString();
    }
  }

  public enum Formats
  {
    JSON("json"),
    AVRO_WITHOUT_SCHEMA_REGISTRY("avro_without_schema_registry"),
    AVRO_WITH_SCHEMA_REGISTRY("avro_with_schema_registry");

    private final String name;

    Formats(String name)
    {
      this.name = name;
    }

    @Override
    public String toString()
    {
      return name;
    }
  }

  public enum Tables
  {
    ONE_G_TABLE("one_g_table", 4000000),
    THREE_HUNDRED_COLUMN_TABLE("three_hundred_column_table", 100000);

    private final String name;
    private final int size;
    private final String schema;

    Tables(String name, int size)
    {
      final String schemaDir = "schema/";

      this.name = name;
      this.size = size;
      this.schema = Utils.loadSchema(schemaDir + name + ".json").toString();
    }

    public Schema getSchema()
    {
      Schema.Parser parser = new Schema.Parser();
      return parser.parse(schema);
    }
    public int getSize()
    {
      return size;
    }

    @Override
    public String toString()
    {
      return name;
    }
  }
}

