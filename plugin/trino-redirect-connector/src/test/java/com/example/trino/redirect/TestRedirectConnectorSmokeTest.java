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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for the Redirect Connector.
 *
 * These tests verify basic functionality of the redirect connector including:
 * - Schema listing
 * - Table redirection
 * - Query execution through redirected tables
 * - Integration with other catalogs
 */
class TestRedirectConnectorSmokeTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedirectQueryRunner.createQueryRunner();
    }

    @Test
    void testShowSchemas()
    {
        // Virtual catalog should show only the two virtual schemas
        assertQuery(
                "SHOW SCHEMAS FROM virtual",
                "VALUES ('virtual_sales'), ('virtual_data')");
    }

    @Test
    void testShowSchemasDoesNotIncludePhysicalSchemas()
    {
        // Should NOT show physical schemas like 'production', 'analytics', etc.
        assertQueryReturnsEmptyResult("SELECT * FROM information_schema.schemata " +
                "WHERE catalog_name = 'virtual' AND schema_name IN ('production', 'analytics', 'public')");
    }

    @Test
    void testShowTablesReturnsEmpty()
    {
        // listTables returns empty - tables are discovered through redirection
        assertQueryReturnsEmptyResult("SHOW TABLES FROM virtual.virtual_sales");
        assertQueryReturnsEmptyResult("SHOW TABLES FROM virtual.virtual_data");
    }

    @Test
    void testSelectFromRedirectedTable()
    {
        // Query virtual_sales.daily_orders which redirects to hive.production.fact_orders_daily
        assertQuery(
                "SELECT orderkey, custkey, orderstatus FROM virtual.virtual_sales.daily_orders ORDER BY orderkey LIMIT 5",
                "SELECT orderkey, custkey, orderstatus FROM hive.production.fact_orders_daily ORDER BY orderkey LIMIT 5");
    }

    @Test
    void testSelectFromAnotherRedirectedTable()
    {
        // Query virtual_data.user_profiles which redirects to iceberg.analytics.dim_users
        assertQuery(
                "SELECT user_id, user_name FROM virtual.virtual_data.user_profiles ORDER BY user_id LIMIT 5",
                "SELECT user_id, user_name FROM iceberg.analytics.dim_users ORDER BY user_id LIMIT 5");
    }

    @Test
    void testCountFromRedirectedTable()
    {
        // COUNT should work through redirection
        assertQuery(
                "SELECT COUNT(*) FROM virtual.virtual_sales.daily_orders",
                "SELECT COUNT(*) FROM hive.production.fact_orders_daily");
    }

    @Test
    void testAggregationOnRedirectedTable()
    {
        // Aggregation queries should work
        assertQuery(
                "SELECT orderstatus, COUNT(*) as cnt FROM virtual.virtual_sales.daily_orders GROUP BY orderstatus ORDER BY orderstatus",
                "SELECT orderstatus, COUNT(*) as cnt FROM hive.production.fact_orders_daily GROUP BY orderstatus ORDER BY orderstatus");
    }

    @Test
    void testJoinRedirectedTables()
    {
        // Join between two virtual tables (redirected to different physical catalogs)
        assertQuery("""
                SELECT o.orderkey, o.totalprice, c.user_name
                FROM virtual.virtual_sales.daily_orders o
                JOIN virtual.virtual_data.user_profiles c ON o.custkey = c.user_id
                ORDER BY o.orderkey
                LIMIT 10
                """);
    }

    @Test
    void testJoinVirtualAndPhysicalTable()
    {
        // Join virtual table with a physical TPCH table
        assertQuery("""
                SELECT v.orderkey, v.totalprice, n.name as nation
                FROM virtual.virtual_sales.daily_orders v
                JOIN tpch.tiny.customer c ON v.custkey = c.custkey
                JOIN tpch.tiny.nation n ON c.nationkey = n.nationkey
                ORDER BY v.orderkey
                LIMIT 10
                """);
    }

    @Test
    void testFilterOnRedirectedTable()
    {
        // WHERE clause should work
        assertQuery(
                "SELECT orderkey FROM virtual.virtual_sales.daily_orders WHERE orderstatus = 'F' ORDER BY orderkey LIMIT 5",
                "SELECT orderkey FROM hive.production.fact_orders_daily WHERE orderstatus = 'F' ORDER BY orderkey LIMIT 5");
    }

    @Test
    void testComplexQueryOnRedirectedTable()
    {
        // Complex query with multiple operations
        assertQuery("""
                SELECT
                    orderstatus,
                    COUNT(*) as order_count,
                    SUM(totalprice) as total_revenue,
                    AVG(totalprice) as avg_price
                FROM virtual.virtual_sales.daily_orders
                WHERE totalprice > 100000
                GROUP BY orderstatus
                ORDER BY orderstatus
                """);
    }

    @Test
    void testDescribeRedirectedTable()
    {
        // DESCRIBE should work (metadata comes from physical table)
        assertQuery(
                "DESCRIBE virtual.virtual_sales.daily_orders",
                "DESCRIBE hive.production.fact_orders_daily");
    }

    @Test
    void testShowColumnsFromRedirectedTable()
    {
        // SHOW COLUMNS should work
        assertQuery(
                "SHOW COLUMNS FROM virtual.virtual_sales.daily_orders",
                "SHOW COLUMNS FROM hive.production.fact_orders_daily");
    }

    @Test
    void testSelectAllRedirectedTables()
    {
        // Verify all mapped tables work
        assertQuerySucceeds("SELECT * FROM virtual.virtual_sales.daily_orders LIMIT 1");
        assertQuerySucceeds("SELECT * FROM virtual.virtual_sales.monthly_revenue LIMIT 1");
        assertQuerySucceeds("SELECT * FROM virtual.virtual_sales.customer_segments LIMIT 1");
        assertQuerySucceeds("SELECT * FROM virtual.virtual_data.user_profiles LIMIT 1");
        assertQuerySucceeds("SELECT * FROM virtual.virtual_data.activity_logs LIMIT 1");
        assertQuerySucceeds("SELECT * FROM virtual.virtual_data.product_catalog LIMIT 1");
    }

    @Test
    void testNonExistentTable()
    {
        // Querying a non-existent virtual table should fail
        assertQueryFails(
                "SELECT * FROM virtual.virtual_sales.non_existent_table",
                ".*Table.*not found.*");
    }

    @Test
    void testNonExistentSchema()
    {
        // Querying from a non-virtual schema should fail
        assertQueryFails(
                "SELECT * FROM virtual.non_virtual_schema.some_table",
                ".*Schema.*not found.*");
    }

    @Test
    void testSubqueryWithRedirection()
    {
        // Subquery should work
        assertQuery("""
                SELECT orderkey, totalprice
                FROM virtual.virtual_sales.daily_orders
                WHERE custkey IN (
                    SELECT user_id
                    FROM virtual.virtual_data.user_profiles
                    WHERE segment = 'BUILDING'
                )
                ORDER BY orderkey
                LIMIT 5
                """);
    }

    @Test
    void testCTEWithRedirection()
    {
        // Common Table Expression (CTE) should work
        assertQuery("""
                WITH high_value_orders AS (
                    SELECT *
                    FROM virtual.virtual_sales.daily_orders
                    WHERE totalprice > 200000
                )
                SELECT COUNT(*) as high_value_count
                FROM high_value_orders
                """);
    }

    @Test
    void testUnionAcrossRedirectedTables()
    {
        // UNION between redirected tables
        assertQuery("""
                SELECT orderkey as id, 'order' as type
                FROM virtual.virtual_sales.daily_orders
                LIMIT 5
                UNION ALL
                SELECT user_id as id, 'user' as type
                FROM virtual.virtual_data.user_profiles
                LIMIT 5
                """);
    }

    @Test
    void testCreateTableNotSupported()
    {
        // CREATE TABLE should fail (read-only connector)
        assertQueryFails(
                "CREATE TABLE virtual.virtual_sales.new_table (id INTEGER)",
                ".*not supported.*");
    }

    @Test
    void testInsertNotSupported()
    {
        // INSERT should fail (read-only connector)
        assertQueryFails(
                "INSERT INTO virtual.virtual_sales.daily_orders VALUES (999999, 1, 'O', 100.0, DATE '2024-01-01', '1-URGENT', 'Clerk#000000001', 0, 'test')",
                ".*not supported.*");
    }

    @Test
    void testDropTableNotSupported()
    {
        // DROP TABLE should fail
        assertQueryFails(
                "DROP TABLE virtual.virtual_sales.daily_orders",
                ".*not supported.*");
    }
}
