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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

/**
 * Decides the physical table location based on table metadata retrieved from RPC service.
 *
 * This class implements the table mapping logic inspired by chaintable-offline's TableNameDecider:
 * - Queries the RPC service for table metadata
 * - Determines the physical catalog/schema/table based on table type
 * - Handles special suffixes like ".archived"
 *
 * Table routing rules:
 * - OFFCHAIN tables → online catalog (location-based naming)
 * - ONCHAIN_STATE tables → online catalog normally, offline catalog if archived
 * - ONCHAIN_ITEM tables → offline catalog (ID-based naming)
 */
public class TableNameDecider
{
    private static final Logger log = Logger.get(TableNameDecider.class);
    private static final String ARCHIVED_SUFFIX = ".archived";

    private final TableMappingService tableMappingService;
    private final String offlineCatalog;
    private final String offlineSchema;
    private final String onlineCatalog;
    private final String onlineSchema;

    @Inject
    public TableNameDecider(
            TableMappingService tableMappingService,
            RedirectConfig config)
    {
        this.tableMappingService = tableMappingService;
        this.offlineCatalog = config.getOfflineCatalog();
        this.offlineSchema = config.getOfflineSchema();
        this.onlineCatalog = config.getOnlineCatalog();
        this.onlineSchema = config.getOnlineSchema();

        log.info("TableNameDecider initialized: offline=%s.%s, online=%s.%s",
                offlineCatalog, offlineSchema, onlineCatalog, onlineSchema);
    }

    /**
     * Decides the physical table location for a given virtual table name.
     *
     * @param virtualTableName The virtual table name from the query
     * @return Optional containing the physical table location, or empty if table not found
     */
    public Optional<CatalogSchemaTableName> decideTableMapping(SchemaTableName virtualTableName)
    {
        String tableName = virtualTableName.getTableName();
        log.info("Deciding table mapping for: %s", tableName);

        try {
            // Check for archived table suffix
            boolean isArchived = false;
            if (tableName.endsWith(ARCHIVED_SUFFIX)) {
                tableName = tableName.substring(0, tableName.length() - ARCHIVED_SUFFIX.length());
                isArchived = true;
                log.info("Detected archived table: %s (original: %s)", tableName, virtualTableName.getTableName());
            }

            // Fetch table metadata from RPC service
            TableMetadata metadata = tableMappingService.getTableMetaByName(tableName);
            log.info("Retrieved metadata for %s: type=%s, location=%s, id=%d",
                    tableName, metadata.getType(), metadata.getLocation(), metadata.getId());

            // Determine physical location based on table type
            CatalogSchemaTableName physicalTable = decidePhysicalTable(metadata, isArchived);
            log.info("Decided physical table for %s: %s.%s.%s",
                    virtualTableName,
                    physicalTable.getCatalogName(),
                    physicalTable.getSchemaTableName().getSchemaName(),
                    physicalTable.getSchemaTableName().getTableName());

            return Optional.of(physicalTable);
        }
        catch (TableMappingException e) {
            log.warn(e, "Failed to resolve table mapping for: %s", virtualTableName);
            return Optional.empty();
        }
    }

    /**
     * Determines the physical table location based on table metadata and archive status.
     *
     * Routing logic (inspired by chaintable-offline):
     * - OFFCHAIN: Always use online table (location-based naming)
     * - ONCHAIN_STATE: Use online table normally, offline table if archived
     * - ONCHAIN_ITEM: Always use offline table (ID-based naming)
     */
    private CatalogSchemaTableName decidePhysicalTable(TableMetadata metadata, boolean isArchived)
    {
        return switch (metadata.getType()) {
            case OFFCHAIN ->
                // OFFCHAIN tables always go to online storage
                    useOnlineTable(metadata.getLocation());

            case ONCHAIN_STATE -> {
                if (isArchived) {
                    // Archived ONCHAIN_STATE tables go to offline storage
                    yield useOfflineTable(metadata.getId() + "_archived");
                }
                else {
                    // Active ONCHAIN_STATE tables go to online storage
                    yield useOnlineTable(metadata.getLocation());
                }
            }

            case ONCHAIN_ITEM ->
                // ONCHAIN_ITEM tables always go to offline storage
                    useOfflineTable(String.valueOf(metadata.getId()));
        };
    }

    /**
     * Creates a reference to an online table.
     * Online table names are derived from the location path.
     *
     * Example: location="s3://bucket/data/my_table" → online.default.my_table
     */
    private CatalogSchemaTableName useOnlineTable(String location)
    {
        // Extract table name from location path
        // Example: "s3://bucket/data/my_table" → "my_table"
        String tableName;
        if (location != null && !location.isEmpty()) {
            int lastSlash = location.lastIndexOf('/');
            tableName = (lastSlash >= 0) ? location.substring(lastSlash + 1) : location;
        }
        else {
            throw new IllegalArgumentException("Location is null or empty for online table");
        }

        log.debug("Using online table: %s.%s.%s", onlineCatalog, onlineSchema, tableName);
        return new CatalogSchemaTableName(
                onlineCatalog,
                onlineSchema,
                tableName);
    }

    /**
     * Creates a reference to an offline table.
     * Offline table names are based on the table ID.
     *
     * Example: tableId="12345" → iceberg.offline_data.12345
     */
    private CatalogSchemaTableName useOfflineTable(String tableId)
    {
        log.debug("Using offline table: %s.%s.%s", offlineCatalog, offlineSchema, tableId);
        return new CatalogSchemaTableName(
                offlineCatalog,
                offlineSchema,
                tableId);
    }
}
