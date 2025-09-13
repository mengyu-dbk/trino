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
package io.trino.plugin.uint256;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestUInt256AggregationFunctions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        StandaloneQueryRunner runner = new StandaloneQueryRunner(
                testSessionBuilder().setCatalog("memory").setSchema("default").build());

        // install memory connector and create catalog
        runner.installPlugin(new MemoryPlugin());
        runner.createCatalog("memory", "memory", ImmutableMap.of());

        // install uint256 global type plugin (registers type + functions)
        runner.installPlugin(new UInt256Plugin());
        return runner;
    }

    @Test
    public void testSumBasic()
    {
        // 测试基本的SUM功能 - 转换为十六进制字符串进行比较
        assertQuery(
                "SELECT to_hex(CAST(sum(CAST('1' AS UINT256)) AS varbinary))",
                "VALUES '0000000000000000000000000000000000000000000000000000000000000001'");

        assertQuery(
                "SELECT to_hex(CAST(sum(CAST('100' AS UINT256)) AS varbinary))",
                "VALUES '0000000000000000000000000000000000000000000000000000000000000064'");

        // 测试多个值的SUM
        assertQuery(
                "SELECT to_hex(CAST(sum(val) AS varbinary)) FROM (VALUES CAST('10' AS UINT256), CAST('20' AS UINT256), CAST('30' AS UINT256)) AS t(val)",
                "VALUES '000000000000000000000000000000000000000000000000000000000000003C'");
    }

    @Test
    public void testSumWithNulls()
    {
        // 测试包含NULL的SUM
        assertQuery("SELECT to_hex(CAST(sum(val) AS varbinary)) FROM (VALUES CAST(NULL AS UINT256)) AS t(val)", "VALUES NULL");

        assertQuery(
                "SELECT to_hex(CAST(sum(val) AS varbinary)) FROM (VALUES CAST('10' AS UINT256), CAST(NULL AS UINT256), CAST('20' AS UINT256)) AS t(val)",
                "VALUES '000000000000000000000000000000000000000000000000000000000000001E'");
    }

    @Test
    public void testSumLargeNumbers()
    {
        // 测试大数值的SUM
        assertQuery(
                "SELECT CAST(sum(val) AS VARCHAR) FROM (VALUES " +
                "CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256), " +
                "CAST('0' AS UINT256)) AS t(val)",
                "VALUES '115792089237316195423570985008687907853269984665640564039457584007913129639935'");

        // 测试较大数值相加
        assertQuery(
                "SELECT CAST(sum(val) AS VARCHAR) FROM (VALUES " +
                "CAST('1000000000000000000000000000000' AS UINT256), " +
                "CAST('2000000000000000000000000000000' AS UINT256)) AS t(val)",
                "VALUES '3000000000000000000000000000000'");
    }

    @Test
    public void testAvgBasic()
    {
        // 测试多个值的AVG
        assertQuery(
                "SELECT CAST(avg(val) AS VARCHAR) FROM (VALUES CAST('10' AS UINT256), CAST('20' AS UINT256), CAST('30' AS UINT256)) AS t(val)",
                "VALUES '20'");

        // 测试能被整除的平均值
        assertQuery(
                "SELECT CAST(avg(val) AS VARCHAR) FROM (VALUES CAST('100' AS UINT256), CAST('200' AS UINT256)) AS t(val)",
                "VALUES '150'");
    }

    @Test
    public void testAvgWithNulls()
    {
        // 测试包含NULL的AVG
        assertQuery("SELECT CAST(avg(val) AS VARCHAR) FROM (VALUES CAST(NULL AS UINT256)) AS t(val)", "VALUES NULL");

        assertQuery(
                "SELECT CAST(avg(val) AS VARCHAR) FROM (VALUES CAST('10' AS UINT256), CAST(NULL AS UINT256), CAST('30' AS UINT256)) AS t(val)",
                "VALUES '20'");
    }

    @Test
    public void testEmptySet()
    {
        // 测试空集合的聚合
        assertQuery(
                "SELECT CAST(sum(val) AS VARCHAR), CAST(avg(val) AS VARCHAR) " +
                        "FROM (SELECT CAST(NULL as UINT256) AS val) AS t " +
                        "WHERE val IS NOT NULL",
                "VALUES (CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR))");
    }

    @Test
    public void testMixedWithOtherTypes()
    {
        // 测试与其他类型的混合使用
        assertQuery(
                "SELECT CAST(sum(CAST(bigint_val AS UINT256)) AS VARCHAR) FROM (VALUES 10, 20, 30) AS t(bigint_val)",
                "VALUES '60'");

        assertQuery(
                "SELECT CAST(avg(CAST(bigint_val AS UINT256)) AS VARCHAR) FROM (VALUES 10, 20, 30) AS t(bigint_val)",
                "VALUES '20'");
    }

    @Test
    public void testOverflowBehavior()
    {
        // 测试溢出情况（应该抛出异常）
        assertQueryFails(
                "SELECT CAST(sum(val) AS VARCHAR) FROM (VALUES " +
                "CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256), " +
                "CAST('1' AS UINT256)) AS t(val)",
                ".*overflow.*");
    }
/*
    @Test
    public void testWindowFunction()
    {
        // 测试窗口函数（如果支持的话）
        assertQuery(
                "SELECT val, CAST(sum(val) OVER () AS VARCHAR) AS total_sum, CAST(avg(val) OVER () AS VARCHAR) AS total_avg FROM (" +
                "VALUES CAST('10' AS UINT256), CAST('20' AS UINT256), CAST('30' AS UINT256)" +
                ") AS t(val) ORDER BY val",
                "VALUES " +
                "('10', '60', '20'), " +
                "('20', '60', '20'), " +
                "('30', '60', '20')");
    }
*/

    @Test
    public void testComplexQueries()
    {
        // 测试复杂查询场景
        assertQuery(
                "WITH data AS (" +
                "SELECT CAST('100' AS UINT256) AS val UNION ALL " +
                "SELECT CAST('200' AS UINT256) AS val UNION ALL " +
                "SELECT CAST('300' AS UINT256) AS val" +
                ") " +
                "SELECT CAST(sum(val) AS VARCHAR) AS total, CAST(avg(val) AS VARCHAR) AS average, count(val) AS cnt FROM data",
                "VALUES ('600', '200', 3)");
    }
}
