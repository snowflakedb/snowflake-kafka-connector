package com.snowflake.kafka.connector.testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Client for interacting with Kafka Connect REST API.
 */
final class KafkaConnectApiClient {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOGGER =
        LoggerFactory.getLogger(KafkaConnectApiClient.class);

  private final String connectUrl;
  private final HttpClient httpClient;

  KafkaConnectApiClient(final String connectUrl, final HttpClient httpClient) {
    this.connectUrl = connectUrl;
    this.httpClient = httpClient;
  }

   KafkaConnectPlugin[] getConnectorPlugins() throws IOException, InterruptedException {
    final String endpoint = connectUrl + "/connector-plugins";
    final HttpRequest request = HttpRequest.newBuilder().uri(URI.create(endpoint)).GET().build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException(
          "Failed to get connector plugins. Status: "
              + response.statusCode()
              + ", Response: "
              + response.body());
    }
    return OBJECT_MAPPER.readValue(response.body(), KafkaConnectPlugin[].class);

  }

  void createConnector(final String name, final Map<String, String> config)
      throws IOException, InterruptedException {
    final String endpoint = connectUrl + "/connectors";

    final ObjectNode configJson = OBJECT_MAPPER.createObjectNode();
    configJson.put("name", name);

    final ObjectNode configNode = OBJECT_MAPPER.createObjectNode();
    config.forEach(configNode::put);
    configJson.set("config", configNode);

    final HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(configJson)))
            .build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 201) {
      throw new IOException(
          "Failed to create connector. Status: "
              + response.statusCode()
              + ", Response: "
              + response.body());
    }
  }


  void deleteConnector(final String name) {
      if(name==null || name.isEmpty()){
          LOGGER.warn("Connector name is null or empty, skipping deletion.");
          return;
      }
      try {
          final String endpoint = connectUrl + "/connectors/" + name;
          final HttpRequest request =
              HttpRequest.newBuilder().uri(URI.create(endpoint)).DELETE().build();

          final HttpResponse<String> response =
              httpClient.send(request, HttpResponse.BodyHandlers.ofString());

          if (response.statusCode() != 204 && response.statusCode() != 404) {
              throw new RuntimeException(
                  "Failed to delete connector. Status: "
                      + response.statusCode()
                      + ", Response: "
                      + response.body());
          }
      } catch (IOException | InterruptedException e) {
          throw new RuntimeException("Error deleting connector: " + e.getMessage(), e);
      }
  }
}

