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
package io.trino.plugin.memory;

import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUInt256Integration
{
    private static QueryRunner queryRunner;

    @BeforeAll
    public static void setUp()
            throws Exception
    {
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().build());
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");
    }

    @AfterAll
    public static void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    public void testCreateTableWithUInt256()
    {
        // 创建包含UINT256类型的表
        queryRunner.execute("CREATE TABLE memory.default.test_uint256 (id INTEGER, value UINT256)");

        // 验证表结构
        MaterializedResult result = queryRunner.execute("DESCRIBE memory.default.test_uint256");
        assertThat(result.getRowCount()).isEqualTo(2);

        // 验证列类型
        boolean foundUint256 = false;
        for (MaterializedRow row : result) {
            String columnName = (String) row.getField(0);
            String columnType = (String) row.getField(1);
            if ("value".equals(columnName) && "uint256".equals(columnType)) {
                foundUint256 = true;
                break;
            }
        }
        assertThat(foundUint256).isTrue();

        // 清理
        queryRunner.execute("DROP TABLE memory.default.test_uint256");
    }

    @Test
    public void testInsertAndSelectUInt256()
    {
        // 创建表
        queryRunner.execute("CREATE TABLE memory.default.test_uint256_data (id INTEGER, value UINT256)");

        // 插入数据
        queryRunner.execute("INSERT INTO memory.default.test_uint256_data VALUES (1, X'0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20')");
        queryRunner.execute("INSERT INTO memory.default.test_uint256_data VALUES (2, X'2122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F40')");

        // 查询数据
        MaterializedResult result = queryRunner.execute("SELECT * FROM memory.default.test_uint256_data ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);

        // 验证第一行数据
        MaterializedRow firstRow = result.getMaterializedRows().get(0);
        assertThat(firstRow.getField(0)).isEqualTo(1);
        assertThat(firstRow.getField(1)).isInstanceOf(byte[].class);
        byte[] firstValue = (byte[]) firstRow.getField(1);
        assertThat(firstValue).hasSize(32);
        assertThat(firstValue[0]).isEqualTo((byte) 1);
        assertThat(firstValue[31]).isEqualTo((byte) 32);

        // 验证第二行数据
        MaterializedRow secondRow = result.getMaterializedRows().get(1);
        assertThat(secondRow.getField(0)).isEqualTo(2);
        assertThat(secondRow.getField(1)).isInstanceOf(byte[].class);
        byte[] secondValue = (byte[]) secondRow.getField(1);
        assertThat(secondValue).hasSize(32);
        assertThat(secondValue[0]).isEqualTo((byte) 33);
        assertThat(secondValue[31]).isEqualTo((byte) 64);

        // 清理
        queryRunner.execute("DROP TABLE memory.default.test_uint256_data");
    }

    @Test
    public void testUInt256Comparison()
    {
        // 创建表
        queryRunner.execute("CREATE TABLE memory.default.test_uint256_compare (id INTEGER, value UINT256)");

        // 插入数据
        queryRunner.execute("INSERT INTO memory.default.test_uint256_compare VALUES (1, X'0000000000000000000000000000000000000000000000000000000000000001')");
        queryRunner.execute("INSERT INTO memory.default.test_uint256_compare VALUES (2, X'0000000000000000000000000000000000000000000000000000000000000002')");
        queryRunner.execute("INSERT INTO memory.default.test_uint256_compare VALUES (3, X'0000000000000000000000000000000000000000000000000000000000000003')");

        // 测试比较操作
        MaterializedResult result = queryRunner.execute("SELECT id FROM memory.default.test_uint256_compare WHERE value > X'0000000000000000000000000000000000000000000000000000000000000001' ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);

        // 清理
        queryRunner.execute("DROP TABLE memory.default.test_uint256_compare");
    }
}
