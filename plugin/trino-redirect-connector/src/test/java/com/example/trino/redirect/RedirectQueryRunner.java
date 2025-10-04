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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * QueryRunner for testing the Redirect Connector.
 *
 * This sets up a complete test environment with:
 * - Virtual catalog (redirect connector)
 * - Physical catalogs (hive, iceberg, postgresql - mocked with memory connector)
 * - TPCH catalog for test data
 */
public final class RedirectQueryRunner
{
    private static final Logger log = Logger.get(RedirectQueryRunner.class);

    private static final String VIRTUAL_CATALOG = "virtual";
    private static final String HIVE_CATALOG = "hive";
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String POSTGRESQL_CATALOG = "postgresql";
    private static final String TPCH_CATALOG = "tpch";

    private RedirectQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(VIRTUAL_CATALOG)
                    .setSchema("virtual_sales")
                    .build());
        }

        @Override
        @CanIgnoreReturnValue
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();

            try {
                // Install TPCH plugin for generating test data
                log.info("Installing TPCH plugin");
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog(TPCH_CATALOG, "tpch", ImmutableMap.of());

                // Install Memory plugin to mock physical catalogs (hive, iceberg, postgresql)
                log.info("Installing Memory plugin for physical catalogs");
                queryRunner.installPlugin(new MemoryPlugin());

                // Create "hive" catalog (mocked with memory connector)
                queryRunner.createCatalog(HIVE_CATALOG, "memory", ImmutableMap.of());

                // Create "iceberg" catalog (mocked with memory connector)
                queryRunner.createCatalog(ICEBERG_CATALOG, "memory", ImmutableMap.of());

                // Create "postgresql" catalog (mocked with memory connector)
                queryRunner.createCatalog(POSTGRESQL_CATALOG, "memory", ImmutableMap.of());

                // Set up schemas and test data
                setupTestData(queryRunner);

                // Install Redirect plugin
                log.info("Installing Redirect plugin");
                queryRunner.installPlugin(new RedirectPlugin());
                queryRunner.createCatalog(VIRTUAL_CATALOG, "redirect", ImmutableMap.of());

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }

        private void setupTestData(QueryRunner queryRunner)
        {
            log.info("Setting up test schemas and data");

            // Create schemas in hive catalog
            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS hive.production");
            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS hive.raw_data");

            // Create schemas in iceberg catalog
            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.analytics");

            // Create schemas in postgresql catalog
            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS postgresql.public");

            // Create physical tables with test data
            // These tables will be the targets of redirection

            // hive.production.fact_orders_daily (target for virtual_sales.daily_orders)
            queryRunner.execute("""
                    CREATE TABLE hive.production.fact_orders_daily AS
                    SELECT
                        orderkey,
                        custkey,
                        orderstatus,
                        totalprice,
                        orderdate,
                        orderpriority,
                        clerk,
                        shippriority,
                        comment
                    FROM tpch.tiny.orders
                    """);

            // hive.production.fact_revenue_monthly (target for virtual_sales.monthly_revenue)
            queryRunner.execute("""
                    CREATE TABLE hive.production.fact_revenue_monthly AS
                    SELECT
                        CAST(year(orderdate) AS INTEGER) AS year,
                        CAST(month(orderdate) AS INTEGER) AS month,
                        SUM(totalprice) AS total_revenue,
                        COUNT(*) AS order_count
                    FROM tpch.tiny.orders
                    GROUP BY year(orderdate), month(orderdate)
                    """);

            // iceberg.analytics.dim_customer_segments (target for virtual_sales.customer_segments)
            queryRunner.execute("""
                    CREATE TABLE iceberg.analytics.dim_customer_segments AS
                    SELECT
                        custkey,
                        name,
                        address,
                        phone,
                        acctbal,
                        mktsegment,
                        comment
                    FROM tpch.tiny.customer
                    """);

            // iceberg.analytics.dim_users (target for virtual_data.user_profiles)
            queryRunner.execute("""
                    CREATE TABLE iceberg.analytics.dim_users AS
                    SELECT
                        custkey AS user_id,
                        name AS user_name,
                        address,
                        phone,
                        acctbal AS account_balance,
                        mktsegment AS segment
                    FROM tpch.tiny.customer
                    """);

            // hive.raw_data.fact_user_activity (target for virtual_data.activity_logs)
            queryRunner.execute("""
                    CREATE TABLE hive.raw_data.fact_user_activity AS
                    SELECT
                        orderkey AS activity_id,
                        custkey AS user_id,
                        orderdate AS activity_date,
                        orderstatus AS activity_type,
                        totalprice AS activity_value
                    FROM tpch.tiny.orders
                    """);

            // postgresql.public.products (target for virtual_data.product_catalog)
            queryRunner.execute("""
                    CREATE TABLE postgresql.public.products AS
                    SELECT
                        partkey AS product_id,
                        name AS product_name,
                        mfgr AS manufacturer,
                        brand,
                        type AS product_type,
                        size,
                        container,
                        retailprice
                    FROM tpch.tiny.part
                    """);

            log.info("Test data setup complete");
        }
    }

    /**
     * Creates a default RedirectQueryRunner for testing.
     *
     * @return A configured QueryRunner with redirect and physical catalogs
     */
    public static QueryRunner createQueryRunner()
            throws Exception
    {
        return builder().build();
    }
}
