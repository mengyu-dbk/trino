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

import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for RedirectConnectorMetadata.
 *
 * These tests verify the core metadata operations of the redirect connector,
 * particularly schema listing and table redirection logic.
 */
class TestRedirectConnectorMetadata
{
    private final RedirectConnectorMetadata metadata = new RedirectConnectorMetadata(null);

    @Test
    void testListSchemaNames()
    {
        // Should return only the two virtual schemas
        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas)
                .as("Should return exactly two virtual schemas")
                .hasSize(2)
                .containsExactlyInAnyOrder("virtual_sales", "virtual_data");
    }

    @Test
    void testRedirectVirtualSalesTable()
    {
        // Test redirection for virtual_sales.daily_orders -> hive.production.fact_orders_daily
        SchemaTableName virtualTable = new SchemaTableName("virtual_sales", "daily_orders");
        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(SESSION, virtualTable);

        assertThat(redirect)
                .as("virtual_sales.daily_orders should redirect")
                .isPresent()
                .hasValueSatisfying(target -> {
                    assertThat(target.getCatalogName()).isEqualTo("hive");
                    assertThat(target.getSchemaTableName().getSchemaName()).isEqualTo("production");
                    assertThat(target.getSchemaTableName().getTableName()).isEqualTo("fact_orders_daily");
                });
    }

    @Test
    void testRedirectVirtualDataTable()
    {
        // Test redirection for virtual_data.user_profiles -> iceberg.analytics.dim_users
        SchemaTableName virtualTable = new SchemaTableName("virtual_data", "user_profiles");
        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(SESSION, virtualTable);

        assertThat(redirect)
                .as("virtual_data.user_profiles should redirect")
                .isPresent()
                .hasValueSatisfying(target -> {
                    assertThat(target.getCatalogName()).isEqualTo("iceberg");
                    assertThat(target.getSchemaTableName().getSchemaName()).isEqualTo("analytics");
                    assertThat(target.getSchemaTableName().getTableName()).isEqualTo("dim_users");
                });
    }

    @Test
    void testAllTableMappings()
    {
        // Verify all hardcoded mappings work correctly
        assertRedirection("virtual_sales", "daily_orders", "hive", "production", "fact_orders_daily");
        assertRedirection("virtual_sales", "monthly_revenue", "hive", "production", "fact_revenue_monthly");
        assertRedirection("virtual_sales", "customer_segments", "iceberg", "analytics", "dim_customer_segments");
        assertRedirection("virtual_data", "user_profiles", "iceberg", "analytics", "dim_users");
        assertRedirection("virtual_data", "activity_logs", "hive", "raw_data", "fact_user_activity");
        assertRedirection("virtual_data", "product_catalog", "postgresql", "public", "products");
    }

    @Test
    void testRedirectNonVirtualSchema()
    {
        // Non-virtual schemas should NOT be redirected (returns empty)
        SchemaTableName hiveTable = new SchemaTableName("production", "some_table");
        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(SESSION, hiveTable);

        assertThat(redirect)
                .as("Non-virtual schema should not redirect")
                .isEmpty();
    }

    @Test
    void testRedirectNonExistentTable()
    {
        // Non-existent table in virtual schema should return empty
        SchemaTableName nonExistent = new SchemaTableName("virtual_sales", "non_existent_table");
        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(SESSION, nonExistent);

        assertThat(redirect)
                .as("Non-existent table should not redirect")
                .isEmpty();
    }

    @Test
    void testPreventInfiniteLoop()
    {
        // Physical catalog names should NOT be in virtual schemas
        // This prevents infinite redirection loops

        // Try to redirect a table from "hive" schema (should fail since it's not virtual)
        SchemaTableName physicalTable = new SchemaTableName("hive", "some_table");
        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(SESSION, physicalTable);

        assertThat(redirect)
                .as("Physical catalog schema should not redirect")
                .isEmpty();

        // Try "iceberg" schema
        SchemaTableName icebergTable = new SchemaTableName("iceberg", "some_table");
        redirect = metadata.redirectTable(SESSION, icebergTable);

        assertThat(redirect)
                .as("Iceberg schema should not redirect")
                .isEmpty();
    }

    @Test
    void testListTablesReturnsEmpty()
    {
        // listTables returns empty - tables are discovered through redirection
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of("virtual_sales"));

        assertThat(tables)
                .as("listTables should return empty (tables discovered via redirection)")
                .isEmpty();

        // Test with no schema filter
        tables = metadata.listTables(SESSION, Optional.empty());

        assertThat(tables)
                .as("listTables with no filter should return empty")
                .isEmpty();
    }

    @Test
    void testRoleManagementNotSupported()
    {
        // Role management operations should not be supported
        assertThat(metadata.roleExists(SESSION, "some_role"))
                .as("roleExists should return false")
                .isFalse();

        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                        () -> metadata.createRole(SESSION, "test_role", Optional.empty())))
                .as("createRole should throw UnsupportedOperationException")
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Role management is not supported");

        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                        () -> metadata.dropRole(SESSION, "test_role")))
                .as("dropRole should throw UnsupportedOperationException")
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Role management is not supported");
    }

    /**
     * Helper method to assert table redirection.
     */
    private void assertRedirection(
            String virtualSchema,
            String virtualTable,
            String targetCatalog,
            String targetSchema,
            String targetTable)
    {
        SchemaTableName source = new SchemaTableName(virtualSchema, virtualTable);
        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(SESSION, source);

        assertThat(redirect)
                .as(String.format("%s.%s should redirect to %s.%s.%s",
                        virtualSchema, virtualTable, targetCatalog, targetSchema, targetTable))
                .isPresent()
                .hasValueSatisfying(target -> {
                    assertThat(target.getCatalogName()).isEqualTo(targetCatalog);
                    assertThat(target.getSchemaTableName().getSchemaName()).isEqualTo(targetSchema);
                    assertThat(target.getSchemaTableName().getTableName()).isEqualTo(targetTable);
                });
    }
}
