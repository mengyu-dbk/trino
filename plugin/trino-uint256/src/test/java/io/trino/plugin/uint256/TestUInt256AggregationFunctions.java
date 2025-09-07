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
/*
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
                "SELECT sum(val) FROM (VALUES " +
                "CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256), " +
                "CAST('0' AS UINT256)) AS t(val)",
                "SELECT CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256)");

        // 测试较大数值相加
        assertQuery(
                "SELECT sum(val) FROM (VALUES " +
                "CAST('1000000000000000000000000000000' AS UINT256), " +
                "CAST('2000000000000000000000000000000' AS UINT256)) AS t(val)",
                "SELECT CAST('3000000000000000000000000000000' AS UINT256)");
    }

    @Test
    public void testAvgBasic()
    {
        // 测试基本的AVG功能
        assertQuery("SELECT avg(CAST('10' AS UINT256))", "SELECT CAST('10' AS UINT256)");

        // 测试多个值的AVG
        assertQuery(
                "SELECT avg(val) FROM (VALUES CAST('10' AS UINT256), CAST('20' AS UINT256), CAST('30' AS UINT256)) AS t(val)",
                "SELECT CAST('20' AS UINT256)");

        // 测试能被整除的平均值
        assertQuery(
                "SELECT avg(val) FROM (VALUES CAST('100' AS UINT256), CAST('200' AS UINT256)) AS t(val)",
                "SELECT CAST('150' AS UINT256)");
    }

    @Test
    public void testAvgWithNulls()
    {
        // 测试包含NULL的AVG
        assertQuery("SELECT avg(val) FROM (VALUES CAST(NULL AS UINT256)) AS t(val)", "SELECT CAST(NULL AS UINT256)");

        assertQuery(
                "SELECT avg(val) FROM (VALUES CAST('10' AS UINT256), CAST(NULL AS UINT256), CAST('30' AS UINT256)) AS t(val)",
                "SELECT CAST('20' AS UINT256)");
    }

    @Test
    public void testAvgTruncation()
    {
        // 测试AVG的截断行为（整数除法）
        assertQuery(
                "SELECT avg(val) FROM (VALUES CAST('1' AS UINT256), CAST('2' AS UINT256)) AS t(val)",
                "SELECT CAST('1' AS UINT256)"); // 1.5 截断为 1

        assertQuery(
                "SELECT avg(val) FROM (VALUES CAST('5' AS UINT256), CAST('7' AS UINT256), CAST('9' AS UINT256)) AS t(val)",
                "SELECT CAST('7' AS UINT256)"); // 21/3 = 7
    }

    @Test
    public void testAvgLargeNumbers()
    {
        // 测试大数值的AVG
        assertQuery(
                "SELECT avg(val) FROM (VALUES " +
                "CAST('1000000000000000000000000000000' AS UINT256), " +
                "CAST('3000000000000000000000000000000' AS UINT256)) AS t(val)",
                "SELECT CAST('2000000000000000000000000000000' AS UINT256)");
    }

    @Test
    public void testGroupedAggregation()
    {
        // 测试分组聚合
        assertQuery(
                "SELECT grp, sum(val), avg(val) FROM (" +
                "VALUES " +
                "(1, CAST('10' AS UINT256)), " +
                "(1, CAST('20' AS UINT256)), " +
                "(2, CAST('30' AS UINT256)), " +
                "(2, CAST('40' AS UINT256))" +
                ") AS t(grp, val) GROUP BY grp ORDER BY grp",
                "VALUES " +
                "(1, CAST('30' AS UINT256), CAST('15' AS UINT256)), " +
                "(2, CAST('70' AS UINT256), CAST('35' AS UINT256))");
    }

    @Test
    public void testEmptySet()
    {
        // 测试空集合的聚合
        assertQuery(
                "SELECT sum(val), avg(val) FROM (SELECT CAST(NULL AS UINT256) AS val) AS t WHERE val IS NOT NULL",
                "SELECT CAST(NULL AS UINT256), CAST(NULL AS UINT256)");
    }

    @Test
    public void testMixedWithOtherTypes()
    {
        // 测试与其他类型的混合使用
        assertQuery(
                "SELECT sum(CAST(bigint_val AS UINT256)) FROM (VALUES 10, 20, 30) AS t(bigint_val)",
                "SELECT CAST('60' AS UINT256)");

        assertQuery(
                "SELECT avg(CAST(bigint_val AS UINT256)) FROM (VALUES 10, 20, 30) AS t(bigint_val)",
                "SELECT CAST('20' AS UINT256)");
    }

    @Test
    public void testOverflowBehavior()
    {
        // 测试溢出情况（应该抛出异常）
        assertQueryFails(
                "SELECT sum(val) FROM (VALUES " +
                "CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256), " +
                "CAST('1' AS UINT256)) AS t(val)",
                ".*overflow.*");
    }

    @Test
    public void testWindowFunction()
    {
        // 测试窗口函数（如果支持的话）
        assertQuery(
                "SELECT val, sum(val) OVER () AS total_sum, avg(val) OVER () AS total_avg FROM (" +
                "VALUES CAST('10' AS UINT256), CAST('20' AS UINT256), CAST('30' AS UINT256)" +
                ") AS t(val) ORDER BY val",
                "VALUES " +
                "(CAST('10' AS UINT256), CAST('60' AS UINT256), CAST('20' AS UINT256)), " +
                "(CAST('20' AS UINT256), CAST('60' AS UINT256), CAST('20' AS UINT256)), " +
                "(CAST('30' AS UINT256), CAST('60' AS UINT256), CAST('20' AS UINT256))");
    }

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
                "SELECT sum(val) AS total, avg(val) AS average, count(val) AS cnt FROM data",
                "SELECT CAST('600' AS UINT256), CAST('200' AS UINT256), 3");
    }
 */
}
