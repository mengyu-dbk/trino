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
package io.trino.plugin.iceberg;

import io.trino.plugin.uint256.UInt256Plugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for ALTER TABLE operations with UINT256 columns in Iceberg tables.
 *
 * This test class verifies that UINT256 type information is correctly preserved
 * when adding columns via ALTER TABLE ADD COLUMN.
 */
public class TestUInt256IcebergAlterTable
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .build();

        // Install UINT256 plugin
        queryRunner.installPlugin(new UInt256Plugin());

        return queryRunner;
    }

    @Test
    public void testAlterTableAddUInt256Column()
    {
        String tableName = "test_alter_table_add_uint256_column";

        try {
            // Create a table with a non-UINT256 column
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT)");

            // Add a UINT256 column via ALTER TABLE
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN value UINT256");

            // Verify the column type is UINT256, not VARBINARY
            MaterializedResult result = computeActual("DESCRIBE " + tableName);

            // Check that 'value' column shows UINT256 type
            String resultStr = result.toString();
            assertThat(resultStr).contains("value");
            assertThat(resultStr).containsIgnoringCase("UINT256");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterTableAddUInt256ColumnWithComment()
    {
        String tableName = "test_alter_table_add_uint256_with_comment";

        try {
            // Create a table
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT)");

            // Add a UINT256 column with a comment
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN hash UINT256 COMMENT 'Transaction hash'");

            // Verify the column type is UINT256 and comment is preserved
            MaterializedResult result = computeActual("DESCRIBE " + tableName);

            // The marker should be prepended to the comment
            String resultStr = result.toString();
            assertThat(resultStr).contains("hash");
            assertThat(resultStr).containsIgnoringCase("UINT256");
            assertThat(resultStr).contains("Transaction hash");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterTableAddMultipleUInt256Columns()
    {
        String tableName = "test_alter_table_add_multiple_uint256";

        try {
            // Create a table with one column
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT)");

            // Add multiple UINT256 columns
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN value1 UINT256");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN value2 UINT256");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN value3 UINT256");

            // Verify all columns are UINT256
            MaterializedResult result = computeActual("DESCRIBE " + tableName);
            String resultStr = result.toString();

            // All UINT256 columns should show correct type
            assertThat(resultStr).containsIgnoringCase("value1");
            assertThat(resultStr).containsIgnoringCase("value2");
            assertThat(resultStr).containsIgnoringCase("value3");
            // Count occurrences of UINT256 (should be 3)
            int count = resultStr.split("(?i)UINT256", -1).length - 1;
            assertThat(count).isGreaterThanOrEqualTo(3);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterTableAddUInt256ColumnWithData()
    {
        String tableName = "test_alter_table_add_uint256_with_data";

        try {
            // Create a table and insert data
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1), (2), (3)", 3);

            // Add a UINT256 column to a table with existing data
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN hash UINT256");

            // Verify the column type is UINT256
            MaterializedResult describeResult = computeActual("DESCRIBE " + tableName);
            assertThat(describeResult.toString()).containsIgnoringCase("hash");
            assertThat(describeResult.toString()).containsIgnoringCase("UINT256");

            // Verify we can insert UINT256 values
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, CAST(from_hex('FF') AS UINT256))", 1);

            // Verify we can read the data
            MaterializedResult selectResult = computeActual("SELECT id FROM " + tableName + " WHERE hash IS NOT NULL");
            assertThat(selectResult.getOnlyValue()).isEqualTo(4L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterTableAddUInt256ColumnMixedTypes()
    {
        String tableName = "test_alter_table_mixed_types";

        try {
            // Create a table with various types
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)");

            // Add UINT256 and other types
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN hash UINT256");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN count INTEGER");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN value2 UINT256");

            // Verify all types are correct
            MaterializedResult result = computeActual("DESCRIBE " + tableName);
            String resultStr = result.toString();

            assertThat(resultStr).containsIgnoringCase("id");
            assertThat(resultStr).containsIgnoringCase("bigint");
            assertThat(resultStr).containsIgnoringCase("name");
            assertThat(resultStr).containsIgnoringCase("varchar");
            assertThat(resultStr).containsIgnoringCase("hash");
            assertThat(resultStr).containsIgnoringCase("count");
            assertThat(resultStr).containsIgnoringCase("integer");
            assertThat(resultStr).containsIgnoringCase("value2");
            // Should have 2 UINT256 columns
            int count = resultStr.split("(?i)UINT256", -1).length - 1;
            assertThat(count).isGreaterThanOrEqualTo(2);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterTableAddUInt256ColumnFirst()
    {
        String tableName = "test_alter_table_add_uint256_first";

        try {
            // Create a table
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)");

            // Add a UINT256 column at the beginning
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN hash UINT256 WITH (position = 'FIRST')");

            // Verify the column is first and has UINT256 type
            MaterializedResult result = computeActual("DESCRIBE " + tableName);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("hash");
            assertThat(result.getMaterializedRows().get(0).getField(1).toString()).containsIgnoringCase("UINT256");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterTableAddUInt256ColumnAfter()
    {
        String tableName = "test_alter_table_add_uint256_after";

        try {
            // Create a table
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR, value DOUBLE)");

            // Add a UINT256 column after a specific column
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN hash UINT256 WITH (position = 'AFTER name')");

            // Verify the column position and type
            MaterializedResult result = computeActual("DESCRIBE " + tableName);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("id");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("name");
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("hash");
            assertThat(result.getMaterializedRows().get(2).getField(1).toString()).containsIgnoringCase("UINT256");
            assertThat(result.getMaterializedRows().get(3).getField(0)).isEqualTo("value");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterTableAddUInt256NestedField()
    {
        String tableName = "test_alter_table_add_uint256_nested";

        try {
            // Create a table with a ROW column
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, data ROW(name VARCHAR))");

            // Add a UINT256 field to the ROW column
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN data.hash UINT256");

            // Verify the nested field type
            MaterializedResult result = computeActual("DESCRIBE " + tableName);
            String resultStr = result.toString();

            // The ROW type should contain UINT256
            assertThat(resultStr).containsIgnoringCase("data");
            assertThat(resultStr).containsIgnoringCase("UINT256");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
