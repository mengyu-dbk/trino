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
 * Tests for redirect connector with multiple catalog types.
 *
 * These tests verify that the redirect connector works correctly when
 * redirecting to different types of physical catalogs (Hive, Iceberg, PostgreSQL).
 */
class TestRedirectWithMultipleCatalogs
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedirectQueryRunner.createQueryRunner();
    }

    @Test
    void testJoinAcrossDifferentCatalogTypes()
    {
        // Join tables that redirect to different catalog types
        // virtual_sales.daily_orders -> hive
        // virtual_data.user_profiles -> iceberg
        assertQuerySucceeds("""
                SELECT
                    o.orderkey,
                    o.totalprice,
                    u.user_name,
                    u.segment
                FROM virtual.virtual_sales.daily_orders o
                JOIN virtual.virtual_data.user_profiles u ON o.custkey = u.user_id
                WHERE o.totalprice > 200000
                ORDER BY o.totalprice DESC
                LIMIT 10
                """);
    }

    @Test
    void testThreeWayJoinAcrossCatalogs()
    {
        // Join tables from hive, iceberg, and postgresql (all via redirect)
        assertQuerySucceeds("""
                SELECT
                    o.orderkey,
                    u.user_name,
                    p.product_name
                FROM virtual.virtual_sales.daily_orders o           -- hive
                JOIN virtual.virtual_data.user_profiles u           -- iceberg
                    ON o.custkey = u.user_id
                JOIN virtual.virtual_data.product_catalog p         -- postgresql
                    ON MOD(o.orderkey, 200000) + 1 = p.product_id
                LIMIT 5
                """);
    }

    @Test
    void testUnionAcrossDifferentCatalogTypes()
    {
        // UNION across different catalog types
        assertQuerySucceeds("""
                SELECT * FROM (
                    SELECT custkey as id, orderstatus as status
                    FROM virtual.virtual_sales.daily_orders
                    WHERE orderstatus = 'F'
                    LIMIT 10
                ) UNION ALL
                SELECT * FROM (
                    SELECT user_id as id, segment as status
                    FROM virtual.virtual_data.user_profiles
                    WHERE segment = 'BUILDING'
                    LIMIT 10
                )
                """);
    }

    @Test
    void testAggregationOnEachCatalogType()
    {
        // Verify aggregation works on tables from each catalog type

        // Hive catalog
        assertQuerySucceeds("""
                SELECT orderstatus, COUNT(*) as cnt
                FROM virtual.virtual_sales.daily_orders
                GROUP BY orderstatus
                """);

        // Iceberg catalog
        assertQuerySucceeds("""
                SELECT segment, COUNT(*) as cnt
                FROM virtual.virtual_data.user_profiles
                GROUP BY segment
                """);

        // PostgreSQL catalog
        assertQuerySucceeds("""
                SELECT brand, COUNT(*) as cnt
                FROM virtual.virtual_data.product_catalog
                GROUP BY brand
                LIMIT 10
                """);
    }

    @Test
    void testSubqueryAcrossCatalogs()
    {
        // Subquery joining different catalog types
        assertQuerySucceeds("""
                SELECT
                    orderkey,
                    totalprice,
                    (SELECT user_name
                     FROM virtual.virtual_data.user_profiles u
                     WHERE u.user_id = o.custkey) as customer_name
                FROM virtual.virtual_sales.daily_orders o
                WHERE totalprice > 250000
                ORDER BY totalprice DESC
                LIMIT 5
                """);
    }

    @Test
    void testComplexQueryWithAllCatalogTypes()
    {
        // Complex analytical query across all catalog types
        assertQuerySucceeds("""
                WITH high_value_customers AS (
                    SELECT user_id, user_name, segment
                    FROM virtual.virtual_data.user_profiles
                    WHERE account_balance > 5000
                ),
                recent_orders AS (
                    SELECT custkey, COUNT(*) as order_count, SUM(totalprice) as total_spent
                    FROM virtual.virtual_sales.daily_orders
                    GROUP BY custkey
                )
                SELECT
                    c.user_name,
                    c.segment,
                    COALESCE(r.order_count, 0) as order_count,
                    COALESCE(r.total_spent, 0) as total_spent
                FROM high_value_customers c
                LEFT JOIN recent_orders r ON c.user_id = r.custkey
                ORDER BY total_spent DESC
                LIMIT 20
                """);
    }

    @Test
    void testCrossJoinDifferentCatalogs()
    {
        // CROSS JOIN (Cartesian product) across different catalogs
        assertQuerySucceeds("""
                SELECT COUNT(*)
                FROM (SELECT * FROM virtual.virtual_sales.monthly_revenue LIMIT 5) mr
                CROSS JOIN (SELECT * FROM virtual.virtual_data.product_catalog LIMIT 5) p
                """);
    }

    @Test
    void testWindowFunctionsAcrossCatalogs()
    {
        // Window functions on redirected tables
        assertQuerySucceeds("""
                SELECT
                    orderkey,
                    custkey,
                    totalprice,
                    ROW_NUMBER() OVER (PARTITION BY custkey ORDER BY totalprice DESC) as rank_in_customer
                FROM virtual.virtual_sales.daily_orders
                LIMIT 50
                """);
    }

    @Test
    void testDistinctAcrossCatalogs()
    {
        // DISTINCT across joined tables from different catalogs
        assertQuerySucceeds("""
                SELECT DISTINCT u.segment
                FROM virtual.virtual_sales.daily_orders o
                JOIN virtual.virtual_data.user_profiles u ON o.custkey = u.user_id
                """);
    }

    @Test
    void testGroupByHavingAcrossCatalogs()
    {
        // GROUP BY with HAVING across different catalogs
        assertQuerySucceeds("""
                SELECT
                    u.segment,
                    COUNT(o.orderkey) as order_count,
                    AVG(o.totalprice) as avg_order_value
                FROM virtual.virtual_sales.daily_orders o
                JOIN virtual.virtual_data.user_profiles u ON o.custkey = u.user_id
                GROUP BY u.segment
                HAVING COUNT(o.orderkey) > 100
                ORDER BY order_count DESC
                """);
    }

    @Test
    void testInSubqueryAcrossCatalogs()
    {
        // IN subquery across different catalog types
        assertQuerySucceeds("""
                SELECT product_name, retailprice
                FROM virtual.virtual_data.product_catalog
                WHERE product_id IN (
                    SELECT MOD(orderkey, 200000) + 1
                    FROM virtual.virtual_sales.daily_orders
                    WHERE totalprice > 300000
                )
                LIMIT 20
                """);
    }

    @Test
    void testExistsSubqueryAcrossCatalogs()
    {
        // EXISTS subquery across different catalogs
        assertQuerySucceeds("""
                SELECT user_id, user_name
                FROM virtual.virtual_data.user_profiles u
                WHERE EXISTS (
                    SELECT 1
                    FROM virtual.virtual_sales.daily_orders o
                    WHERE o.custkey = u.user_id
                    AND o.totalprice > 200000
                )
                LIMIT 20
                """);
    }

    @Test
    void testLeftJoinAcrossCatalogs()
    {
        // LEFT JOIN to verify null handling across catalogs
        assertQuerySucceeds("""
                SELECT
                    p.product_name,
                    COUNT(a.activity_id) as activity_count
                FROM virtual.virtual_data.product_catalog p
                LEFT JOIN virtual.virtual_data.activity_logs a
                    ON MOD(a.activity_id, 200000) + 1 = p.product_id
                GROUP BY p.product_name
                LIMIT 20
                """);
    }

    @Test
    void testFullOuterJoinAcrossCatalogs()
    {
        // FULL OUTER JOIN across different catalog types
        assertQuerySucceeds("""
                SELECT
                    COALESCE(o.orderstatus, 'NO_ORDERS') as status,
                    COALESCE(u.segment, 'NO_SEGMENT') as segment,
                    COUNT(*) as cnt
                FROM (SELECT DISTINCT orderstatus, custkey FROM virtual.virtual_sales.daily_orders LIMIT 100) o
                FULL OUTER JOIN (SELECT DISTINCT segment, user_id FROM virtual.virtual_data.user_profiles LIMIT 100) u
                    ON o.custkey = u.user_id
                GROUP BY ROLLUP(o.orderstatus, u.segment)
                LIMIT 50
                """);
    }

    @Test
    void testOrderByFromDifferentCatalogs()
    {
        // ORDER BY columns from different redirected catalogs
        assertQuerySucceeds("""
                SELECT
                    o.orderkey,
                    o.totalprice,
                    u.user_name,
                    u.account_balance
                FROM virtual.virtual_sales.daily_orders o
                JOIN virtual.virtual_data.user_profiles u ON o.custkey = u.user_id
                ORDER BY o.totalprice DESC, u.account_balance DESC
                LIMIT 10
                """);
    }

    @Test
    void testMixedVirtualAndPhysicalTables()
    {
        // Mix of virtual (redirected) and physical (direct) tables
        assertQuerySucceeds("""
                SELECT
                    v.orderkey,
                    v.totalprice,
                    t.orderstatus,
                    n.name as nation
                FROM virtual.virtual_sales.daily_orders v          -- redirected to hive
                JOIN tpch.tiny.orders t ON v.orderkey = t.orderkey -- direct physical table
                JOIN tpch.tiny.customer c ON v.custkey = c.custkey
                JOIN tpch.tiny.nation n ON c.nationkey = n.nationkey
                LIMIT 10
                """);
    }

    @Test
    void testCaseExpressionAcrossCatalogs()
    {
        // CASE expression with data from different catalogs
        assertQuerySucceeds("""
                SELECT
                    o.orderkey,
                    CASE
                        WHEN o.totalprice > 300000 THEN 'HIGH'
                        WHEN o.totalprice > 150000 THEN 'MEDIUM'
                        ELSE 'LOW'
                    END as price_tier,
                    u.segment
                FROM virtual.virtual_sales.daily_orders o
                JOIN virtual.virtual_data.user_profiles u ON o.custkey = u.user_id
                LIMIT 20
                """);
    }

    @Test
    void testSetOperationsAcrossCatalogs()
    {
        // INTERSECT across different catalog types
        assertQuerySucceeds("""
                SELECT custkey FROM virtual.virtual_sales.daily_orders
                INTERSECT
                SELECT user_id FROM virtual.virtual_data.user_profiles
                """);

        // EXCEPT across different catalog types
        assertQuerySucceeds("""
                SELECT custkey FROM virtual.virtual_sales.daily_orders
                EXCEPT
                SELECT user_id FROM virtual.virtual_data.user_profiles
                WHERE segment = 'AUTOMOBILE'
                """);
    }
}
