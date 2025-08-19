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
        assertUpdate("CREATE TABLE memory.default.uint256_test (id INTEGER, v uint256)");

        // insert 3 rows: 1, 2, NULL
        assertUpdate("INSERT INTO memory.default.uint256_test VALUES " +
                "(1, CAST(from_hex('01') AS uint256))," +
                "(2, CAST(from_hex('02') AS uint256))," +
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
        assertUpdate("CREATE TABLE memory.default.uint256_add (id INTEGER, v uint256)");
        assertUpdate("INSERT INTO memory.default.uint256_add VALUES " +
                "(1, CAST(from_hex('01') AS uint256))," +
                "(2, CAST(from_hex('FF') AS uint256))," +
                // max value (32 bytes of FF)
                "(3, CAST(from_hex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') AS uint256))", 3);

        // v + 01
        assertQueryOrdered(
                "SELECT id, to_hex(CAST(v + CAST(from_hex('01') AS uint256) AS varbinary)) FROM memory.default.uint256_add WHERE id IN (1,2) ORDER BY id",
                // 0x01 + 0x01 = 0x02; 0xFF + 0x01 = 0x0100 (32 bytes, carry)
                "VALUES (1, '0000000000000000000000000000000000000000000000000000000000000002')," +
                        "(2, '0000000000000000000000000000000000000000000000000000000000000100')");

        // overflow: max + 01 -> error
        assertQueryFails(
                "SELECT to_hex(CAST(v + CAST(from_hex('01') AS uint256) AS varbinary)) FROM memory.default.uint256_add WHERE id = 3",
                ".*uint256 addition overflow.*");

        // null propagation
        assertUpdate("INSERT INTO memory.default.uint256_add VALUES (4, NULL)", 1);
        assertQueryReturnsEmptyResult(
                "SELECT to_hex(CAST(v + CAST(from_hex('01') AS uint256) AS varbinary)) FROM memory.default.uint256_add WHERE id = 4 AND v IS NOT NULL");
    }

    @Test
    public void testPredicatesAndOrdering()
    {
        assertUpdate("CREATE TABLE memory.default.uint256_pred (id INTEGER, v uint256)");
        assertUpdate("INSERT INTO memory.default.uint256_pred VALUES " +
                "(1, CAST(from_hex('10') AS uint256))," +
                "(2, CAST(from_hex('0F') AS uint256))," +
                "(3, CAST(from_hex('0100') AS uint256))", 3);

        // WHERE and ORDER BY
        assertQueryOrdered(
                "SELECT to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_pred WHERE v > CAST(from_hex('0F') AS uint256) ORDER BY v",
                "VALUES ('0000000000000000000000000000000000000000000000000000000000000010')," +
                        "('0000000000000000000000000000000000000000000000000000000000000100')");
    }
}
