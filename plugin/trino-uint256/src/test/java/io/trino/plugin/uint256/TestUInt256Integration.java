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
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestUInt256Integration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner runner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog("memory").setSchema("default").build())
                .build();

        // install memory connector and create catalog
        runner.installPlugin(new MemoryPlugin());
        runner.createCatalog("memory", "memory", ImmutableMap.of());

        // install uint256 global type plugin (registers type + functions)
        runner.installPlugin(new UInt256Plugin());
        return runner;
    }

    @Test
    public void testCreateTableInsertAndQuery()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_test (id INTEGER, v UINT256)");

        // insert 3 rows: 1, 2, NULL
        assertUpdate("INSERT INTO memory.default.uint256_test VALUES " +
                "(1, CAST(from_hex('01') AS UINT256))," +
                "(2, CAST(from_hex('02') AS UINT256))," +
                "(3, NULL)", 3);

        // basic select and order by
        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_test WHERE v IS NOT NULL ORDER BY id",
                "VALUES (1, '0000000000000000000000000000000000000000000000000000000000000001')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000002')");

        // null handling
        assertQuery("SELECT count(*) FROM memory.default.uint256_test WHERE v IS NULL", "VALUES 1");
    }

    @Test
    public void testAddition()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_add (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_add VALUES " +
                "(1, CAST(from_hex('01') AS UINT256))," +
                "(2, CAST(from_hex('FF') AS UINT256))," +
                // max value (32 bytes of FF)
                "(3, CAST(from_hex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') AS UINT256))", 3);

        // v + 01
        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v + CAST(from_hex('01') AS UINT256) AS varbinary)) FROM memory.default.uint256_add WHERE id IN (1,2) ORDER BY id",
                // 0x01 + 0x01 = 0x02; 0xFF + 0x01 = 0x0100 (32 bytes, carry)
                "VALUES (1, '0000000000000000000000000000000000000000000000000000000000000002')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000100')");

        // overflow: max + 01 -> error
        assertQueryFails(
                "SELECT to_hex(CAST(v + CAST(from_hex('01') AS UINT256) AS varbinary)) FROM memory.default.uint256_add WHERE id = 3",
                ".*uint256 addition overflow.*");

        // null propagation
        assertUpdate("INSERT INTO memory.default.uint256_add VALUES (4, NULL)", 1);
        assertQueryReturnsEmptyResult(
                "SELECT to_hex(CAST(v + CAST(from_hex('01') AS UINT256) AS varbinary)) FROM memory.default.uint256_add WHERE id = 4 AND v IS NOT NULL");
    }

    @Test
    public void testBigintToUint256CastAndInsert()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_bigint (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_bigint VALUES (1, CAST(CAST(123456789 AS BIGINT) AS UINT256)), (2, uint256(CAST(987654321 AS BIGINT)))", 2);

        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_bigint ORDER BY id",
                "VALUES (1, '00000000000000000000000000000000000000000000000000000000075BCD15')," +
                "(2, '000000000000000000000000000000000000000000000000000000003ADE68B1')");
    }

    @Test
    public void testPredicatesAndOrdering()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_pred (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_pred VALUES " +
                "(1, CAST(from_hex('10') AS UINT256))," +
                "(2, CAST(from_hex('0F') AS UINT256))," +
                "(3, CAST(from_hex('0100') AS UINT256))", 3);

        // WHERE and ORDER BY
        assertQueryOrdered(
                "SELECT to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_pred WHERE v > CAST(from_hex('0F') AS UINT256) ORDER BY v",
                "VALUES ('0000000000000000000000000000000000000000000000000000000000000010')," +
                        "('0000000000000000000000000000000000000000000000000000000000000100')");
    }

    @Test
    public void testVarcharCasts()
    {
        // 正向：VARCHAR -> UINT256（含0x前缀、大小写、奇数字符自动补0）
        assertQuery(
                "SELECT to_hex(CAST(CAST('1' AS UINT256) AS varbinary))",
                "VALUES '0000000000000000000000000000000000000000000000000000000000000001'");
        assertQuery(
                "SELECT to_hex(CAST(CAST('255' AS UINT256) AS varbinary))",
                "VALUES '00000000000000000000000000000000000000000000000000000000000000FF'");
        assertQuery(
                "SELECT to_hex(CAST(CAST('15' AS UINT256) AS varbinary))",
                "VALUES '000000000000000000000000000000000000000000000000000000000000000F'");
        // 错误：非法字符
        assertQueryFails("SELECT CAST('0xz1' AS UINT256)", ".*Invalid UINT256 value:.*");
        // 错误：数字过大
        String longHex = "9".repeat(100);
        assertQueryFails("SELECT CAST('" + longHex + "' AS UINT256)", ".*uint256 value out of range.*");

        // 错误：负数
        assertQueryFails("SELECT CAST('-1' AS UINT256)", ".*uint256 value out of range*");
        // 错误：空字符串
        assertQueryFails("SELECT CAST('' AS UINT256)", ".*Invalid UINT256 value:.*");
        // 反向：UINT256 -> VARCHAR（固定64位小写hex）
        assertQuery(
                "SELECT CAST(CAST(from_hex('0A0B') AS UINT256) AS VARCHAR)",
                "VALUES '2571'");
    }

    @Test
    public void testSubMulDiv()
    {
        // subtraction 正常
        assertQuery(
                "SELECT to_hex(CAST(CAST(from_hex('0100') AS UINT256) - CAST(from_hex('01') AS UINT256) AS varbinary))",
                "VALUES '00000000000000000000000000000000000000000000000000000000000000FF'");
        // subtraction 下溢
        assertQueryFails(
                "SELECT CAST(from_hex('00') AS UINT256) - CAST(from_hex('01') AS UINT256)",
                ".*uint256 subtraction underflow.*");

        // multiply 正常
        assertQuery(
                "SELECT to_hex(CAST(CAST(from_hex('02') AS UINT256) * CAST(from_hex('03') AS UINT256) AS varbinary))",
                "VALUES '0000000000000000000000000000000000000000000000000000000000000006'");
        // multiply 上溢
        assertQueryFails(
                "SELECT CAST(from_hex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') AS UINT256) * CAST(from_hex('02') AS UINT256)",
                ".*uint256 multiplication overflow.*");

        // divide 正常
        assertQuery(
                "SELECT to_hex(CAST(CAST(from_hex('10') AS UINT256) / CAST(from_hex('04') AS UINT256) AS varbinary))",
                "VALUES '0000000000000000000000000000000000000000000000000000000000000004'");
        // divide by zero
        assertQueryFails(
                "SELECT CAST(from_hex('10') AS UINT256) / CAST(from_hex('00') AS UINT256)",
                ".*Division by zero.*");
    }

    @Test
    public void testBitwiseFunctions()
    {
        // and
        assertQuery(
                "SELECT to_hex(CAST(bitwise_and(CAST(from_hex('F0') AS UINT256), CAST(from_hex('0F') AS UINT256)) AS varbinary))",
                "VALUES '0000000000000000000000000000000000000000000000000000000000000000'");
        // or
        assertQuery(
                "SELECT to_hex(CAST(bitwise_or(CAST(from_hex('F0') AS UINT256), CAST(from_hex('0F') AS UINT256)) AS varbinary))",
                "VALUES '00000000000000000000000000000000000000000000000000000000000000FF'");
        // xor
        assertQuery(
                "SELECT to_hex(CAST(bitwise_xor(CAST(from_hex('F0') AS UINT256), CAST(from_hex('0F') AS UINT256)) AS varbinary))",
                "VALUES '00000000000000000000000000000000000000000000000000000000000000FF'");
        // not
        assertQuery(
                "SELECT to_hex(CAST(bitwise_not(CAST(from_hex('00FF') AS UINT256)) AS varbinary))",
                // ~00FF => leading FFs then FF00
                "VALUES 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00'");
    }
}
