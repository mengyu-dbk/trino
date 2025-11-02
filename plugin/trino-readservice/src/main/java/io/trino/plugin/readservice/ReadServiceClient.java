/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.readservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.trino.plugin.readservice.models.QueryResult;
import io.trino.plugin.readservice.models.TableMetadata;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ReadServiceClient
{
    private final URI readServiceEndpoint;
    private final URI metaServiceEndpoint;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    @Inject
    public ReadServiceClient(ReadServiceConfig config)
    {
        requireNonNull(config, "config is null");
        this.readServiceEndpoint = config.getReadServiceEndpoint();
        this.metaServiceEndpoint = config.getMetaServiceEndpoint();
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Execute SQL query via ReadService API
     *
     * @param sql SQL query to execute
     * @return QueryResult containing data and metadata
     */
    public QueryResult executeQuery(String sql)
    {
        requireNonNull(sql, "sql is null");
        try {
            URI queryUri = readServiceEndpoint.resolve("/api/v1/table/query");

            String requestBody = objectMapper.writeValueAsString(Map.of("sql", sql));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(queryUri)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("ReadService query failed with status: " + response.statusCode() + ", body: " + response.body());
            }

            return objectMapper.readValue(response.body(), QueryResult.class);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute query via ReadService", e);
        }
    }

    /**
     * Get table ID by table name from MetaService
     *
     * @param tableName Full table name (e.g., "chaintable.block.eth")
     * @return Table ID
     */
    public long getTableIdByName(String tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try {
            String uriString = metaServiceEndpoint + "/api/v1/meta/get_id_by_name?name=" + tableName;
            URI uri = new URI(uriString);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("MetaService getIdByName failed with status: " + response.statusCode());
            }

            Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
            return ((Number) result.get("id")).longValue();
        }
        catch (IOException | InterruptedException | URISyntaxException e) {
            throw new RuntimeException("Failed to get table ID from MetaService", e);
        }
    }

    /**
     * Get table metadata by table ID from MetaService
     *
     * @param tableId Table ID
     * @return Table metadata including fields
     */
    public TableMetadata getTableMetadata(long tableId)
    {
        try {
            String uriString = metaServiceEndpoint + "/api/v1/meta/get_table_metadata?id=" + tableId;
            URI uri = new URI(uriString);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("MetaService getTableMetadata failed with status: " + response.statusCode());
            }

            return objectMapper.readValue(response.body(), TableMetadata.class);
        }
        catch (IOException | InterruptedException | URISyntaxException e) {
            throw new RuntimeException("Failed to get table metadata from MetaService", e);
        }
    }

    /**
     * Get table metadata by table name (convenience method)
     *
     * @param tableName Full table name
     * @return Table metadata
     */
    public TableMetadata getTableMetadataByName(String tableName)
    {
        long tableId = getTableIdByName(tableName);
        return getTableMetadata(tableId);
    }
}
