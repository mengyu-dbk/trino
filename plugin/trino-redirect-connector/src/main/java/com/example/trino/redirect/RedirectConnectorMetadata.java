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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Metadata implementation for the Redirect Connector with gRPC-based table mapping.
 *
 * This is the core of the plugin that implements table redirection logic using:
 * - TableNameDecider: Decides physical table location based on RPC metadata
 * - TableMappingService: gRPC client for fetching table metadata (with caching)
 *
 * Key features:
 * - Defines virtual schemas (virtual_sales, virtual_data)
 * - Implements redirectTable() to map virtual tables to physical tables via RPC
 * - Prevents infinite redirection loops by only redirecting within virtual schemas
 *
 * Inspired by chaintable-offline's architecture:
 * - Uses gRPC for table metadata lookup
 * - Caches results for performance
 * - Determines physical location based on table type
 */
public class RedirectConnectorMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(RedirectConnectorMetadata.class);

    /**
     * Set of virtual schema names that this connector manages.
     * ONLY tables in these schemas will be redirected.
     * This is the primary mechanism to prevent infinite redirection loops.
     */
    private static final Set<String> VIRTUAL_SCHEMAS = Set.of(
            "virtual_sales",
            "virtual_data");

    private final TableNameDecider tableNameDecider;

    /**
     * Creates RedirectConnectorMetadata with Guice-injected dependencies.
     *
     * @param tableNameDecider The table name decider that uses RPC service to determine physical tables
     */
    @Inject
    public RedirectConnectorMetadata(TableNameDecider tableNameDecider)
    {
        this.tableNameDecider = requireNonNull(tableNameDecider, "tableNameDecider is null");
        log.info("RedirectConnectorMetadata initialized with virtual schemas: %s", VIRTUAL_SCHEMAS);
    }

    /**
     * Lists all schemas in this catalog.
     *
     * Returns only the predefined virtual schemas (virtual_sales, virtual_data).
     * This ensures that SHOW SCHEMAS FROM virtual; displays the correct schemas.
     *
     * @param session The connector session
     * @return List of virtual schema names
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(VIRTUAL_SCHEMAS);
    }

    /**
     * Core redirection logic: maps virtual tables to physical tables using gRPC-based lookup.
     *
     * This method is called by Trino's query planner when resolving table references.
     * When a query references a table (e.g., SELECT * FROM virtual.virtual_sales.daily_orders),
     * this method determines if the table should be redirected to another catalog.
     *
     * Flow:
     * 1. Check if the schema is one of our virtual schemas
     *    - If NOT, return Optional.empty() immediately (no redirection)
     *    - This prevents infinite loops and scopes redirection to our virtual schemas only
     *
     * 2. Call TableNameDecider to determine physical table location
     *    - TableNameDecider uses TableMappingService (gRPC + cache)
     *    - Determines location based on table type (OFFCHAIN, ONCHAIN_STATE, ONCHAIN_ITEM)
     *
     * 3. If a mapping exists, return the physical table location
     *    - Otherwise, return Optional.empty() (table doesn't exist in this virtual schema)
     *
     * @param session The connector session
     * @param tableName The virtual table name being queried
     * @return Optional containing the physical table location, or empty if no redirection
     */
    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        String schemaName = tableName.getSchemaName();
        String tableNameStr = tableName.getTableName();

        log.info("=== REDIRECT PLUGIN: Checking table: %s.%s", schemaName, tableNameStr);

        // CRITICAL: Only redirect tables in our virtual schemas
        // This prevents infinite loops and unwanted redirection
        if (!VIRTUAL_SCHEMAS.contains(schemaName)) {
            log.info("=== REDIRECT PLUGIN: Schema '%s' is not a virtual schema, no redirection", schemaName);
            return Optional.empty();
        }

        // Use TableNameDecider to determine physical table location via RPC
        // This replaces the hardcoded TABLE_REDIRECTS map
        Optional<CatalogSchemaTableName> physicalTable = tableNameDecider.decideTableMapping(tableName);

        if (physicalTable.isPresent()) {
            CatalogSchemaTableName physical = physicalTable.get();
            log.info("=== REDIRECT PLUGIN: ✓ Redirecting %s.%s -> %s.%s.%s",
                    schemaName, tableNameStr,
                    physical.getCatalogName(),
                    physical.getSchemaTableName().getSchemaName(),
                    physical.getSchemaTableName().getTableName());
        }
        else {
            log.info("=== REDIRECT PLUGIN: ✗ No mapping found for %s.%s", schemaName, tableNameStr);
        }

        // Return the physical table if found, otherwise empty
        // Empty means: "this table doesn't exist in this virtual schema"
        return physicalTable;
    }

    /**
     * Lists all tables in a schema.
     *
     * For this simple implementation, we return an empty list.
     * In a production implementation, you might want to:
     * - Return the list of virtual tables available in the schema
     * - Call an RPC endpoint to get the list of tables
     * - Read from a metadata store
     *
     * Returning empty list means SHOW TABLES FROM virtual.virtual_sales; will show no tables,
     * but direct queries to known tables will still work via redirectTable().
     *
     * @param session The connector session
     * @param schemaName The schema name (or null for all schemas)
     * @return Empty list (tables are discovered through redirection only)
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        // Return empty - tables are discovered through redirection
        // Alternatively, you could return TABLE_REDIRECTS.keySet() filtered by schema
        return ImmutableList.of();
    }

    /**
     * Determines if a role exists.
     *
     * This connector doesn't manage roles, so always return false.
     *
     * @param session The connector session
     * @param role The role name
     * @return false (no role management)
     */
    @Override
    public boolean roleExists(ConnectorSession session, String role)
    {
        return false;
    }

    /**
     * Creates a new role.
     *
     * This connector doesn't support role management.
     * Throw UnsupportedOperationException to indicate this operation is not supported.
     *
     * @param session The connector session
     * @param role The role name to create
     * @param grantor The principal granting the role
     */
    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        throw new UnsupportedOperationException("Role management is not supported");
    }

    /**
     * Drops a role.
     *
     * This connector doesn't support role management.
     * Throw UnsupportedOperationException to indicate this operation is not supported.
     *
     * @param session The connector session
     * @param role The role name to drop
     */
    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        throw new UnsupportedOperationException("Role management is not supported");
    }
}
