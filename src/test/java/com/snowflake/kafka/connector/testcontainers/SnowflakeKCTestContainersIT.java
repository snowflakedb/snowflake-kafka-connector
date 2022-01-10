package com.snowflake.kafka.connector.testcontainers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import net.christophschubert.cp.testcontainers.KafkaConnectContainer;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

public class SnowflakeKCTestContainersIT {
  private static Network network = Network.newNetwork();

  protected static final OkHttpClient CLIENT = new OkHttpClient();

  public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private static ObjectMapper MAPPER = new ObjectMapper();

  private static String SF_CONFLUENT_HUB = "snowflakeinc/snowflake-kafka-connector:1.6.3";

  private static String SF_CONNECTOR_CONFIG_FILENAME = "sf-connector-test.json";

  @Test
  public void simpleSnowflakeConnectorContainerTest() throws IOException, URISyntaxException {
    final CPTestContainerFactory factory = new CPTestContainerFactory(network);

    // implicitly starts kafka
    final KafkaContainer kafka = factory.createKafka();
    final KafkaConnectContainer connect =
        factory.createCustomConnector(Set.of(SF_CONFLUENT_HUB), kafka);
    connect.start();

    String json = readFileFromResources(SF_CONNECTOR_CONFIG_FILENAME);
    ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(json);

    Map<String, String> testConfig = TestUtils.getConf();

    jsonNode.put("snowflake.url.name", testConfig.get("snowflake.url.name"));
    jsonNode.put("snowflake.user.name", testConfig.get("snowflake.user.name"));
    jsonNode.put("snowflake.private.key", testConfig.get("snowflake.private.key"));
    jsonNode.put("snowflake.database.name", testConfig.get("snowflake.database.name"));
    jsonNode.put("snowflake.schema.name", testConfig.get("snowflake.schema.name"));

    ObjectNode newNode = MAPPER.createObjectNode();

    // Create new node with name and config as two keys
    newNode.set("name", jsonNode.get("name"));
    newNode.set("config", jsonNode);

    registerConnector(MAPPER.writeValueAsString(newNode), jsonNode.get("name").asText(), connect);

    System.out.println(getConnectUrl(connect));
  }

  public String getConnectUrl(KafkaConnectContainer connectContainer) {
    return String.format(
        "http://%s:%s/connectors",
        connectContainer.getHost(), connectContainer.getMappedPort(8083));
  }

  public void registerConnector(
      String payload, String connectorName, KafkaConnectContainer kcContainer) {
    this.executePOSTRequestSuccessfully(payload, this.getConnectUrl(kcContainer));
    Awaitility.await()
        .atMost((long) waitTimeForRecords() * 5L, TimeUnit.SECONDS)
        .until(() -> this.isConnectorConfigured(connectorName, kcContainer));
  }

  public static int waitTimeForRecords() {
    return 2;
  }

  protected static Response executeGETRequest(Request request) {
    try {
      return CLIENT.newCall(request).execute();
    } catch (IOException ex) {
      throw new RuntimeException("Error connecting to Debezium container", ex);
    }
  }

  public String getConnectorUri(String connectorName, KafkaConnectContainer kafkaConnectContainer) {
    return this.getConnectUrl(kafkaConnectContainer) + "/" + connectorName;
  }

  public boolean isConnectorConfigured(
      String connectorName, KafkaConnectContainer kafkaConnectContainer) {
    Request request =
        (new Request.Builder())
            .url(this.getConnectorUri(connectorName, kafkaConnectContainer))
            .build();
    Response response = executeGETRequest(request);

    boolean isResponseSuccessful;
    try {
      isResponseSuccessful = response.isSuccessful();
    } catch (Throwable ex) {
      if (response != null) {
        try {
          response.close();
        } catch (Throwable nestedEx) {
          ex.addSuppressed(nestedEx);
        }
      }

      throw ex;
    }

    if (response != null) {
      response.close();
    }

    return isResponseSuccessful;
  }

  private void executePOSTRequestSuccessfully(String payload, String fullUrl) {
    RequestBody body = RequestBody.create(payload, JSON);
    Request request = (new Request.Builder()).url(fullUrl).post(body).build();

    try {
      Response response = CLIENT.newCall(request).execute();

      try {
        if (!response.isSuccessful()) {
          handleFailedResponse(response);
        }
      } catch (Throwable ex) {
        if (response != null) {
          try {
            response.close();
          } catch (Throwable nestedEx) {
            ex.addSuppressed(nestedEx);
          }
        }

        throw ex;
      }

      if (response != null) {
        response.close();
      }

    } catch (IOException var10) {
      throw new RuntimeException("Error connecting to Debezium container", var10);
    }
  }

  private static void handleFailedResponse(Response response) {
    String responseBodyContent = "{empty response body}";

    try {
      ResponseBody responseBody = response.body();

      try {
        if (null != responseBody) {
          responseBodyContent = responseBody.string();
        }

        throw new IllegalStateException(
            "Unexpected response: " + response + " ; Response Body: " + responseBodyContent);
      } catch (Throwable ex) {
        if (responseBody != null) {
          try {
            responseBody.close();
          } catch (Throwable nestedEx) {
            ex.addSuppressed(nestedEx);
          }
        }

        throw ex;
      }
    } catch (IOException ex) {
      throw new RuntimeException("Error connecting to Debezium container", ex);
    }
  }

  public static String readFileFromResources(String filename)
      throws URISyntaxException, IOException {
    URL resource = SnowflakeKCTestContainersIT.class.getClassLoader().getResource(filename);
    byte[] bytes = Files.readAllBytes(Paths.get(resource.toURI()));
    return new String(bytes);
  }
}
