package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;

public class FetchSchemaClient {
    private static CachedSchemaRegistryClient srClient;
    // the singleton instance
    private static FetchSchemaClient instance;

    private FetchSchemaClient() {
        // private constructor to prevent instantiation
        // Fetch URL from environment variable or default to local schema registry
        // CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL
        String url = System.getenv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL");
        if (url == null || url.isEmpty()) {
            url = "http://localhost:8081"; // Default schema registry URL
        }
        srClient = new CachedSchemaRegistryClient(url,1_000);
    }

    public static FetchSchemaClient getInstance() {
        if (instance == null) {
            instance = new FetchSchemaClient();
        }
        return instance;
    }

    public static void close() {
        try {
            srClient.close();
        } catch (Exception e) {
            // Log the exception or handle it as needed
            e.printStackTrace();
        }
    }

    public Schema getSchema(String subject) {
        try {
            String schemaStr = srClient.getLatestSchemaMetadata(subject).getSchema();
            return new Schema.Parser().parse(schemaStr);
        } catch (Exception e) {
            // Log the exception or handle it as needed
            e.printStackTrace();
            return null;
        }
    }
}
