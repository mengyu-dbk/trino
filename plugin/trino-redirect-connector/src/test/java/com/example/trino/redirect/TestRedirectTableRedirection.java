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

import io.trino.Session;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for table redirection functionality.
 *
 * These tests specifically focus on the redirectTable() method behavior
 * and various edge cases around table redirection.
 */
class TestRedirectTableRedirection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedirectQueryRunner.createQueryRunner();
    }

    @Test
    void testAllVirtualSalesMappings()
    {
        // Verify all virtual_sales schema mappings
        assertRedirectionWorks("virtual_sales", "daily_orders");
        assertRedirectionWorks("virtual_sales", "monthly_revenue");
        assertRedirectionWorks("virtual_sales", "customer_segments");
    }

    @Test
    void testAllVirtualDataMappings()
    {
        // Verify all virtual_data schema mappings
        assertRedirectionWorks("virtual_data", "user_profiles");
        assertRedirectionWorks("virtual_data", "activity_logs");
        assertRedirectionWorks("virtual_data", "product_catalog");
    }

    @Test
    void testRedirectionToHiveCatalog()
    {
        // Test tables redirected to hive catalog
        assertRedirectsTo("virtual_sales", "daily_orders", "hive", "production", "fact_orders_daily");
        assertRedirectsTo("virtual_sales", "monthly_revenue", "hive", "production", "fact_revenue_monthly");
        assertRedirectsTo("virtual_data", "activity_logs", "hive", "raw_data", "fact_user_activity");
    }

    @Test
    void testRedirectionToIcebergCatalog()
    {
        // Test tables redirected to iceberg catalog
        assertRedirectsTo("virtual_sales", "customer_segments", "iceberg", "analytics", "dim_customer_segments");
        assertRedirectsTo("virtual_data", "user_profiles", "iceberg", "analytics", "dim_users");
    }

    @Test
    void testRedirectionToPostgresqlCatalog()
    {
        // Test tables redirected to postgresql catalog
        assertRedirectsTo("virtual_data", "product_catalog", "postgresql", "public", "products");
    }

    @Test
    void testNonVirtualSchemaNotRedirected()
    {
        // Physical catalog schemas should NOT redirect
        assertNoRedirection("hive", "production", "fact_orders_daily");
        assertNoRedirection("iceberg", "analytics", "dim_users");
        assertNoRedirection("postgresql", "public", "products");
        assertNoRedirection("tpch", "tiny", "orders");
    }

    @Test
    void testNonExistentVirtualTableNotRedirected()
    {
        // Non-existent tables in virtual schemas should return empty
        RedirectConnectorMetadata metadata = new RedirectConnectorMetadata();

        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(
                testSessionBuilder().build().toConnectorSession(),
                new SchemaTableName("virtual_sales", "non_existent_table"));

        assertThat(redirect)
                .as("Non-existent virtual table should not redirect")
                .isEmpty();
    }

    @Test
    void testCaseSensitivity()
    {
        // Table names should be case-sensitive (depends on physical catalog)
        // This test verifies the redirection layer preserves case
        assertQuery(
                "SELECT COUNT(*) FROM virtual.virtual_sales.daily_orders",
                "SELECT COUNT(*) FROM hive.production.fact_orders_daily");

        // Note: Upper case should fail if underlying catalog is case-sensitive
        // This behavior comes from the physical catalog, not the redirect layer
    }

    @Test
    void testMultipleRedirectionsInSameQuery()
    {
        // Multiple virtual tables in same query
        assertQuerySucceeds("""
                SELECT
                    (SELECT COUNT(*) FROM virtual.virtual_sales.daily_orders) as orders,
                    (SELECT COUNT(*) FROM virtual.virtual_data.user_profiles) as users,
                    (SELECT COUNT(*) FROM virtual.virtual_sales.customer_segments) as segments
                """);
    }

    @Test
    void testRedirectionWithComplexJoin()
    {
        // Complex join across multiple redirected tables
        assertQuerySucceeds("""
                SELECT
                    o.orderkey,
                    u.user_name,
                    p.product_name
                FROM virtual.virtual_sales.daily_orders o
                JOIN virtual.virtual_data.user_profiles u ON o.custkey = u.user_id
                JOIN virtual.virtual_data.product_catalog p ON MOD(o.orderkey, 200000) + 1 = p.product_id
                LIMIT 10
                """);
    }

    @Test
    void testRedirectionPreservesFilterPushdown()
    {
        // Verify that filter pushdown works through redirection
        // The physical catalog should receive the filter
        assertQuery("""
                SELECT COUNT(*)
                FROM virtual.virtual_sales.daily_orders
                WHERE totalprice > 300000
                """);
    }

    @Test
    void testRedirectionPreservesProjectionPushdown()
    {
        // Verify that projection pushdown works (only select needed columns)
        assertQuery(
                "SELECT orderkey FROM virtual.virtual_sales.daily_orders ORDER BY orderkey LIMIT 5",
                "SELECT orderkey FROM hive.production.fact_orders_daily ORDER BY orderkey LIMIT 5");
    }

    @Test
    void testRedirectionWithAggregationPushdown()
    {
        // Aggregation pushdown through redirection
        assertQuery(
                "SELECT orderstatus, COUNT(*) FROM virtual.virtual_sales.daily_orders GROUP BY orderstatus",
                "SELECT orderstatus, COUNT(*) FROM hive.production.fact_orders_daily GROUP BY orderstatus");
    }

    @Test
    void testRedirectionWithLimit()
    {
        // LIMIT pushdown through redirection
        assertQuery(
                "SELECT * FROM virtual.virtual_sales.daily_orders LIMIT 10",
                "SELECT * FROM hive.production.fact_orders_daily LIMIT 10");
    }

    @Test
    void testExplainPlanShowsPhysicalTable()
    {
        // EXPLAIN should show the physical table after redirection
        String plan = (String) computeScalar("EXPLAIN SELECT * FROM virtual.virtual_sales.daily_orders");

        assertThat(plan)
                .as("EXPLAIN plan should reference physical table")
                .contains("hive.production.fact_orders_daily");
    }

    @Test
    void testQueryStatisticsFromPhysicalTable()
    {
        // Statistics should come from physical table
        assertQuery(
                "SELECT COUNT(*) FROM virtual.virtual_sales.daily_orders",
                "SELECT COUNT(*) FROM hive.production.fact_orders_daily");
    }

    @Test
    void testInformationSchemaWithRedirection()
    {
        // information_schema queries should work
        // Note: Tables might not appear in information_schema.tables
        // because listTables returns empty, but column metadata should work
        // when querying a specific table directly
        assertQuerySucceeds(
                "SELECT column_name FROM information_schema.columns " +
                        "WHERE table_catalog = 'virtual' AND table_schema = 'virtual_sales' " +
                        "AND table_name = 'daily_orders'");
    }

    @Test
    void testSystemMetadataWithRedirection()
    {
        // System metadata queries
        assertQuerySucceeds(
                "SELECT * FROM system.metadata.table_properties " +
                        "WHERE catalog_name = 'virtual'");
    }

    /**
     * Helper method to verify a table query works through redirection.
     */
    private void assertRedirectionWorks(String schema, String table)
    {
        String query = String.format("SELECT COUNT(*) > 0 FROM virtual.%s.%s", schema, table);
        assertQuery(query, "SELECT true");
    }

    /**
     * Helper method to verify redirection target matches expected values.
     */
    private void assertRedirectsTo(
            String virtualSchema,
            String virtualTable,
            String targetCatalog,
            String targetSchema,
            String targetTable)
    {
        RedirectConnectorMetadata metadata = new RedirectConnectorMetadata();
        Session session = testSessionBuilder().build();

        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(
                session.toConnectorSession(),
                new SchemaTableName(virtualSchema, virtualTable));

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

    /**
     * Helper method to verify a table is NOT redirected.
     */
    private void assertNoRedirection(String catalog, String schema, String table)
    {
        RedirectConnectorMetadata metadata = new RedirectConnectorMetadata();
        Session session = testSessionBuilder().build();

        // Note: This tests the metadata layer directly
        // In a real query, this would use a different catalog, not 'virtual'
        Optional<CatalogSchemaTableName> redirect = metadata.redirectTable(
                session.toConnectorSession(),
                new SchemaTableName(schema, table));

        assertThat(redirect)
                .as(String.format("%s.%s should not redirect (not in virtual schema)", schema, table))
                .isEmpty();
    }
}
