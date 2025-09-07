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

        // Test negative bigint cast failure
        assertQueryFails(
                "SELECT CAST(CAST(-1 AS BIGINT) AS UINT256)",
                ".*Cannot cast negative BIGINT value.*");
    }

    @Test
    public void testIntegerToUint256Cast()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_integer (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_integer VALUES " +
                "(1, CAST(CAST(12345 AS INTEGER) AS UINT256))," +
                "(2, CAST(CAST(0 AS INTEGER) AS UINT256))," +
                "(3, CAST(CAST(2147483647 AS INTEGER) AS UINT256))", 3); // INTEGER max value

        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_integer ORDER BY id",
                "VALUES (1, '0000000000000000000000000000000000000000000000000000000000003039')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000000')," +
                        "(3, '000000000000000000000000000000000000000000000000000000007FFFFFFF')");

        // Test negative integer cast failure
        assertQueryFails(
                "SELECT CAST(CAST(-1 AS INTEGER) AS UINT256)",
                ".*Cannot cast negative INTEGER value.*");
    }

    @Test
    public void testSmallintToUint256Cast()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_smallint (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_smallint VALUES " +
                "(1, CAST(CAST(123 AS SMALLINT) AS UINT256))," +
                "(2, CAST(CAST(0 AS SMALLINT) AS UINT256))," +
                "(3, CAST(CAST(32767 AS SMALLINT) AS UINT256))", 3); // SMALLINT max value

        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_smallint ORDER BY id",
                "VALUES (1, '000000000000000000000000000000000000000000000000000000000000007B')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000000')," +
                        "(3, '0000000000000000000000000000000000000000000000000000000000007FFF')");

        // Test negative smallint cast failure
        assertQueryFails(
                "SELECT CAST(CAST(-1 AS SMALLINT) AS UINT256)",
                ".*Cannot cast negative SMALLINT value.*");
    }

    @Test
    public void testTinyintToUint256Cast()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_tinyint (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_tinyint VALUES " +
                "(1, CAST(CAST(42 AS TINYINT) AS UINT256))," +
                "(2, CAST(CAST(0 AS TINYINT) AS UINT256))," +
                "(3, CAST(CAST(127 AS TINYINT) AS UINT256))", 3); // TINYINT max value

        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_tinyint ORDER BY id",
                "VALUES (1, '000000000000000000000000000000000000000000000000000000000000002A')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000000')," +
                        "(3, '000000000000000000000000000000000000000000000000000000000000007F')");

        // Test negative tinyint cast failure
        assertQueryFails(
                "SELECT CAST(CAST(-1 AS TINYINT) AS UINT256)",
                ".*Cannot cast negative TINYINT value.*");
    }

    @Test
    public void testRealToUint256Cast()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_real (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_real VALUES " +
                "(1, CAST(CAST(123.0 AS REAL) AS UINT256))," +
                "(2, CAST(CAST(0.0 AS REAL) AS UINT256))," +
                "(3, CAST(CAST(1000000.0 AS REAL) AS UINT256))", 3);

        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_real ORDER BY id",
                "VALUES (1, '000000000000000000000000000000000000000000000000000000000000007B')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000000')," +
                        "(3, '00000000000000000000000000000000000000000000000000000000000F4240')");

        // Test negative real cast failure
        assertQueryFails(
                "SELECT CAST(CAST(-1.0 AS REAL) AS UINT256)",
                ".*Cannot cast negative REAL value.*");

        // Test non-integer real cast failure
        assertQueryFails(
                "SELECT CAST(CAST(123.5 AS REAL) AS UINT256)",
                ".*Cannot cast non-integer REAL value.*");

        // Test infinity cast failure
        assertQueryFails(
                "SELECT CAST(CAST(infinity() AS REAL) AS UINT256)",
                ".*Cannot cast non-finite REAL value.*");

        // Test NaN cast failure
        assertQueryFails(
                "SELECT CAST(CAST(nan() AS REAL) AS UINT256)",
                ".*Cannot cast non-finite REAL value.*");
    }

    @Test
    public void testDoubleToUint256Cast()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_double (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_double VALUES " +
                "(1, CAST(CAST(123.0 AS DOUBLE) AS UINT256))," +
                "(2, CAST(CAST(0.0 AS DOUBLE) AS UINT256))," +
                "(3, CAST(CAST(1000000000.0 AS DOUBLE) AS UINT256))", 3);

        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_double ORDER BY id",
                "VALUES (1, '000000000000000000000000000000000000000000000000000000000000007B')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000000')," +
                        "(3, '000000000000000000000000000000000000000000000000000000003B9ACA00')");

        // Test negative double cast failure
        assertQueryFails(
                "SELECT CAST(CAST(-1.0 AS DOUBLE) AS UINT256)",
                ".*Cannot cast negative DOUBLE value.*");

        // Test non-integer double cast failure
        assertQueryFails(
                "SELECT CAST(CAST(123.5 AS DOUBLE) AS UINT256)",
                ".*Cannot cast non-integer DOUBLE value.*");

        // Test infinity cast failure
        assertQueryFails(
                "SELECT CAST(CAST(infinity() AS DOUBLE) AS UINT256)",
                ".*Cannot cast non-finite DOUBLE value.*");

        // Test NaN cast failure
        assertQueryFails(
                "SELECT CAST(CAST(nan() AS DOUBLE) AS UINT256)",
                ".*Cannot cast non-finite DOUBLE value.*");
    }
/*
    @Test
    public void testDecimalToUint256Cast()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_decimal (id INTEGER, v UINT256)");
        assertUpdate("INSERT INTO memory.default.uint256_decimal VALUES " +
                "(1, CAST(CAST(123 AS DECIMAL(10,0)) AS UINT256))," +
                "(2, CAST(CAST(0 AS DECIMAL(10,0)) AS UINT256))," +
                "(3, CAST(CAST(999999999 AS DECIMAL(10,0)) AS UINT256))", 3);

        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_decimal ORDER BY id",
                "VALUES (1, '000000000000000000000000000000000000000000000000000000000000007B')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000000')," +
                        "(3, '000000000000000000000000000000000000000000000000000000003B9AC9FF')");

        // Test negative decimal cast failure
        assertQueryFails(
                "SELECT CAST(CAST(-1 AS DECIMAL(10,0)) AS UINT256)",
                ".*Cannot cast negative DECIMAL value.*");

        // Test non-integer decimal cast failure
        assertQueryFails(
                "SELECT CAST(CAST(123.5 AS DECIMAL(10,1)) AS UINT256)",
                ".*Cannot cast non-integer DECIMAL value.*");
    }
*/
    @Test
    public void testImplicitConversionsWithArithmetic()
    {
        // Test implicit conversions in arithmetic operations according to Dune SQL spec
        assertUpdate("CREATE TABLE memory.default.uint256_mixed (id INTEGER, u UINT256, i INTEGER, b BIGINT)");
        assertUpdate("INSERT INTO memory.default.uint256_mixed VALUES " +
                "(1, CAST(CAST(100 AS BIGINT) AS UINT256), 50, 200)", 1);

        // Test UINT256 + INTEGER (should implicitly convert INTEGER to UINT256)
        assertQuery(
                "SELECT to_hex(CAST(u + CAST(i AS UINT256) AS varbinary)) FROM memory.default.uint256_mixed WHERE id = 1",
                "VALUES '0000000000000000000000000000000000000000000000000000000000000096'"); // 100 + 50 = 150 (0x96)

        // Test UINT256 + BIGINT (should implicitly convert BIGINT to UINT256)
        assertQuery(
                "SELECT to_hex(CAST(u + CAST(b AS UINT256) AS varbinary)) FROM memory.default.uint256_mixed WHERE id = 1",
                "VALUES '000000000000000000000000000000000000000000000000000000000000012C'"); // 100 + 200 = 300 (0x12C)
    }
}
