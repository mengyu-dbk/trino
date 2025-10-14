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

/**
 * Service interface for retrieving table mapping metadata from an external RPC service.
 *
 * This interface abstracts the communication with the external table mapping service.
 * Implementations can use different protocols (gRPC, HTTP REST, etc.).
 *
 * Inspired by chaintable-offline's MetaService interface.
 */
public interface TableMappingService
{
    /**
     * Retrieves table metadata by table name.
     *
     * This method queries the external service to get metadata about a table,
     * which is then used to determine the physical table location.
     *
     * @param tableName The logical table name to look up
     * @return TableMetadata containing the table's physical location information
     * @throws TableMappingException if the table is not found or RPC call fails
     */
    TableMetadata getTableMetaByName(String tableName)
            throws TableMappingException;

    /**
     * Shuts down the service and releases resources (e.g., gRPC channels, connection pools).
     */
    void shutdown();
}
