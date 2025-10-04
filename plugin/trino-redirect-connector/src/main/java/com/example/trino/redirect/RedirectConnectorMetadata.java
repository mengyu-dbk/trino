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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Metadata implementation for the Redirect Connector.
 *
 * This is the core of the plugin that implements table redirection logic.
 * It defines virtual schemas and redirects queries to physical tables in other catalogs.
 *
 * Key features:
 * - Defines virtual schemas (virtual_sales, virtual_data)
 * - Implements redirectTable() to map virtual tables to physical tables
 * - Simulates RPC calls with hardcoded mappings
 * - Prevents infinite redirection loops by only redirecting within virtual schemas
 */
public class RedirectConnectorMetadata
        implements ConnectorMetadata
{
    /**
     * Set of virtual schema names that this connector manages.
     * ONLY tables in these schemas will be redirected.
     * This is the primary mechanism to prevent infinite redirection loops.
     */
    private static final Set<String> VIRTUAL_SCHEMAS = Set.of(
            "virtual_sales",
            "virtual_data");

    /**
     * Simulated RPC mapping from virtual tables to physical tables.
     *
     * In a real implementation, this would be replaced by:
     * - An RPC client calling a remote service
     * - A database lookup
     * - A configuration file
     * - A REST API call
     *
     * Format: "schema.table" -> CatalogSchemaTableName
     */
    private static final Map<String, CatalogSchemaTableName> TABLE_REDIRECTS = ImmutableMap.<String, CatalogSchemaTableName>builder()
            // Virtual sales schema mappings
            .put("virtual_sales.daily_orders", new CatalogSchemaTableName("hive", "production", "fact_orders_daily"))
            .put("virtual_sales.monthly_revenue", new CatalogSchemaTableName("hive", "production", "fact_revenue_monthly"))
            .put("virtual_sales.customer_segments", new CatalogSchemaTableName("iceberg", "analytics", "dim_customer_segments"))
            // Virtual data schema mappings
            .put("virtual_data.user_profiles", new CatalogSchemaTableName("iceberg", "analytics", "dim_users"))
            .put("virtual_data.activity_logs", new CatalogSchemaTableName("hive", "raw_data", "fact_user_activity"))
            .put("virtual_data.product_catalog", new CatalogSchemaTableName("postgresql", "public", "products"))
            .buildOrThrow();

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
     * Core redirection logic: maps virtual tables to physical tables.
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
     * 2. Construct the lookup key (schema.table)
     *
     * 3. Perform the "RPC call" (simulated with a Map lookup)
     *    - In production, this would be: rpcClient.getPhysicalTable(schema, table)
     *
     * 4. If a mapping exists, return the physical table location
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

        // CRITICAL: Only redirect tables in our virtual schemas
        // This prevents infinite loops and unwanted redirection
        if (!VIRTUAL_SCHEMAS.contains(schemaName)) {
            return Optional.empty();
        }

        // Construct the lookup key for the simulated RPC call
        String lookupKey = schemaName + "." + tableNameStr;

        // Simulate RPC call to get physical table location
        // In a real implementation, this would be something like:
        // CatalogSchemaTableName physicalTable = rpcClient.resolveTable(schemaName, tableNameStr);
        CatalogSchemaTableName physicalTable = TABLE_REDIRECTS.get(lookupKey);

        // Return the physical table if found, otherwise empty
        // Empty means: "this table doesn't exist in this virtual schema"
        return Optional.ofNullable(physicalTable);
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
