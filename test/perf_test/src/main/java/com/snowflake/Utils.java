package com.snowflake;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import com.snowflake.jdbc.ConnectionUtils;
import com.snowflake.reporter.*;
import com.snowflake.test.Enums;
import com.snowflake.test.Enums.Formats;

import net.snowflake.client.jdbc.internal.apache.http.HttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.HttpClient;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpDelete;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.HttpClientBuilder;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

public class Utils
{
  private static String LOCAL_TEST = "LOCAL_TEST";
  public static boolean TEST_MODE = Boolean.parseBoolean(System.getenv().getOrDefault(LOCAL_TEST, "true"));
  public static final String REPORT_FILE_PATH = "connector-perf-kafka.json";
  private static final String CONFIG_FILE_PATH = "config/snowflake.json";
  public static final String TEST_TOPIC = "kafka_perf_test";
  public static final String CONNECTOR_NAME = "test";
  public static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonNode serverConfig = null;

  public static JsonNode loadSchema(String fileName)
  {
    try
    {
      return MAPPER.readTree(new File(fileName));
    } catch (IOException e)
    {
      e.printStackTrace();
      System.exit(1);
      return null;
    }
  }

  private static JsonNode getConfig()
  {
    return serverConfig == null ? loadConfig() : serverConfig;
  }

  private static JsonNode loadConfig()
  {
    try
    {
      serverConfig = MAPPER.readTree(new File(CONFIG_FILE_PATH));
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    return serverConfig;
  }

  public static TestCase createTestCase(Enums.TestCases test, Long time)
  {
    return new TestCase(test.toString(), test.getTableName(), time);
  }

  public static TestSuite creaTestSuite(Enums.Formats format, JsonArray cases)
  {
    return new TestSuite(format.toString(), cases);
  }

  private static String runCommand(String command)
  {
    System.out.println("Run Command:\n" + command);
    StringBuilder buff = new StringBuilder();
    Process process;
    try
    {
      process = Runtime.getRuntime().exec(command);
      process.waitFor();
      BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null)
      {
        buff.append(line).append("\n");
      }
      process.destroy();
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
    }

    String result = buff.toString();
    System.out.println("Result: \n" + result);
    return result;
  }

  private static JsonNode postRequest(String endpoint, JsonNode data)
  {
    try
    {
      HttpClient client = HttpClientBuilder.create().build();

      HttpPost request = new HttpPost(endpoint);

      request.setHeader("content-type", "application/json");
      request.setEntity(
        new StringEntity(data.toString())
      );

      HttpResponse response = client.execute(request);

      StringBuilder buff = new StringBuilder();
      BufferedReader reader = new BufferedReader(
        new InputStreamReader(response.getEntity().getContent())
      );
      String line;
      while ((line = reader.readLine()) != null)
      {
        buff.append(line.trim());
      }
      reader.close();
      String result = buff.toString();
      return MAPPER.readTree(result);

    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
      return null;
    }
  }

  private static void deleteRequest(String endpoint)
  {
    try
    {
      HttpClient client = HttpClientBuilder.create().build();

      HttpDelete request = new HttpDelete(endpoint);

      client.execute(request);

    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static void startConfluent()
  {
    runCommand("./confluent/bin/confluent local start");
    runCommand("sleep 10");
  }

  public static void stopConfluent()
  {
    runCommand("./confluent/bin/confluent local destroy");
  }

  public static void createTestTopic()
  {
    runCommand("./confluent/bin/kafka-topics --create --zookeeper " +
      "localhost:2181 --replication-factor 1 --partitions 1 --topic " + TEST_TOPIC);
  }

  public static void startConnector(Formats format)
  {
    ObjectNode data = MAPPER.createObjectNode();
    ObjectNode config = MAPPER.createObjectNode();
    JsonNode server = getConfig().get(CONNECTOR_NAME);


    config.put("connector.class", "com.snowflake.kafka.connector" +
      ".SnowflakeSinkConnector");
    config.put("topics", TEST_TOPIC);
    config.put("snowflake.url.name", server.get(ConnectionUtils.URL).asText());
    config.put("snowflake.user.name",
      server.get(ConnectionUtils.USER).asText());
    config.put("snowflake.private.key",
      server.get(ConnectionUtils.PRIVATE_KEY).asText());
    config.put("snowflake.database.name",
      server.get(ConnectionUtils.DATABASE).asText());
    config.put("snowflake.schema.name",
      server.get(ConnectionUtils.SCHEMA).asText());
    config.put("key.converter", "org.apache.kafka.connect.storage" +
      ".StringConverter");
    config.put("buffer.count.records", 1000000);


    switch (format)
    {
      case JSON:
        config.put("value.converter", "com.snowflake.kafka.connector.records.SnowflakeJsonConverter");
        break;
      case AVRO_WITHOUT_SCHEMA_REGISTRY:
        config.put("value.converter", "com.snowflake.kafka.connector.records.SnowflakeAvroConverterWithoutSchemaRegistry");

        break;
      case AVRO_WITH_SCHEMA_REGISTRY:
        config.put("value.converter", "com.snowflake.kafka.connector.records.SnowflakeAvroConverter");
        config.put("value.converter.schema.registry.url", "http://localhost:8081");
        break;
    }


    data.put("name", "test");
    data.set("config", config);
    
    deleteRequest("http://localhost:8083/connectors/test");

    JsonNode response = postRequest("http://localhost:8083/connectors", data);

    if (response.has("error_code"))
    {
      System.out.println(response.toString());
      System.exit(1);
    }
    else
    {
      System.out.println("Connector started!");
    }

  }

}
