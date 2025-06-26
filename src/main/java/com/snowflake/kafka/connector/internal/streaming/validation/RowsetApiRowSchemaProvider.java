package com.snowflake.kafka.connector.internal.streaming.validation;

import static com.snowflake.kafka.connector.Utils.SF_URL;
import static com.snowflake.kafka.connector.Utils.database;
import static com.snowflake.kafka.connector.Utils.schema;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.HttpClientBuilder;
import net.snowflake.ingest.connection.JWTManager;

/** Provide RowSchema from Rowset API endpoint. */
public class RowsetApiRowSchemaProvider implements RowSchemaProvider {
  private static final KCLogger LOGGER = new KCLogger(RowsetApiRowSchemaProvider.class.getName());

  private static final String TABLE_COLUMNS_ENDPOINT =
      "/v1/streaming/rowsetclient/databases/%s/schemas/%s/tables/%s:table-info";

  private static final String TOKEN_TYPE = "KEYPAIR_JWT";

  private final JWTManager jwtManager;

  public RowsetApiRowSchemaProvider(JWTManager jwtManager) {
    this.jwtManager = jwtManager;
  }

  @Override
  public RowSchema getRowSchema(String tableName, Map<String, String> connectorConfig) {
    HttpPost tableInfoRequest = prepareRequest(tableName, connectorConfig);
    try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
      CloseableHttpResponse response = client.execute(tableInfoRequest);
      RowsetApiColumnsEndpointResponse rowsetApiColumnsEndpointResponse =
          RowsetApiColumnsEndpointResponse.from(response.getEntity().getContent());
      if (rowsetApiColumnsEndpointResponse.getErrorCode() != null) {
        LOGGER.error(
            "Failed to get table schema for table {}. Error code {}",
            tableName,
            rowsetApiColumnsEndpointResponse.getErrorCode());
        throw new RowsetApiException(
            "Failed to get table schema. Error code "
                + rowsetApiColumnsEndpointResponse.getErrorCode());
      }
      List<ColumnMetadata> columns =
          rowsetApiColumnsEndpointResponse.tableColumns().stream()
              .map(RowsetApiRowSchemaProvider::mapColumn)
              .collect(Collectors.toList());
      return new RowSchema(false, columns);
    } catch (IOException e) {
      throw new RowsetApiException(e);
    }
  }

  private static ColumnMetadata mapColumn(RowsetApiColumnsEndpointResponse.TableColumn col) {
    RowsetApiColumnsEndpointResponse.DataType dataType = col.getDataType();
    ColumnMetadata metadata = new ColumnMetadata();
    metadata.setName(col.getColumnName());
    metadata.setPhysicalType(col.getPhysicalType());
    metadata.setNullable(col.isNullable());
    metadata.setCollation(col.getCollation());
    metadata.setType(dataType.getType());
    metadata.setLogicalType(dataType.getType());
    metadata.setSourceIcebergDataType(col.getSourceIcebergDataType());
    if (dataType.getByteLength() != null) {
      metadata.setByteLength(dataType.getByteLength());
    }
    if (dataType.getLength() != null) {
      metadata.setLength(dataType.getLength());
    }
    if (dataType.getPrecision() != null) {
      metadata.setPrecision(dataType.getPrecision());
    }
    if (dataType.getScale() != null) {
      metadata.setScale(dataType.getScale());
    }
    return metadata;
  }

  private HttpPost prepareRequest(String tableName, Map<String, String> connectorConfig) {
    String destinationUrl =
        "https://"
            + connectorConfig.get(SF_URL)
            + String.format(
                TABLE_COLUMNS_ENDPOINT,
                URLEncoder.encode(database(connectorConfig), StandardCharsets.UTF_8),
                URLEncoder.encode(schema(connectorConfig), StandardCharsets.UTF_8),
                URLEncoder.encode(tableName, StandardCharsets.UTF_8));

    HttpPost tableInfoRequest = new HttpPost(destinationUrl);
    tableInfoRequest.addHeader("Authorization", "Bearer " + jwtManager.getToken());
    tableInfoRequest.addHeader("X-Snowflake-Authorization-Token-Type", TOKEN_TYPE);
    tableInfoRequest.addHeader("Content-Type", "application/json");
    tableInfoRequest.addHeader("Accept", "application/json");
    return tableInfoRequest;
  }
}
