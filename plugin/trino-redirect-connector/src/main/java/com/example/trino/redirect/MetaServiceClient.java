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
package com.example.trino.redirect;

import com.example.trino.redirect.models.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Simple HTTP client for MetaService API
 * No caching in this simplified implementation
 */
public class MetaServiceClient
{
    private final String metaServiceEndpoint;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public MetaServiceClient(String metaServiceEndpoint)
    {
        this.metaServiceEndpoint = requireNonNull(metaServiceEndpoint, "metaServiceEndpoint is null");
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Get table ID by table name
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
            throw new RuntimeException("Failed to get table ID from MetaService for table: " + tableName, e);
        }
    }

    /**
     * Get table metadata by table ID
     *
     * @param tableId Table ID
     * @return Table metadata
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
            throw new RuntimeException("Failed to get table metadata from MetaService for tableId: " + tableId, e);
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
