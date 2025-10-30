-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

DROP TABLE IF EXISTS iceberg.default.uint256_view_test_table;

CREATE TABLE iceberg.default.uint256_view_test_table AS
SELECT * FROM (VALUES
    (CAST('1' AS UINT256), CAST('100' AS UINT256)),
    (CAST('2' AS UINT256), CAST('200' AS UINT256)),
    (CAST('3' AS UINT256), CAST('300' AS UINT256)),
    (CAST('4' AS UINT256), CAST('400' AS UINT256)),
    (CAST('5' AS UINT256), CAST('500' AS UINT256))
) t(id, value);

CREATE OR REPLACE VIEW iceberg.default.uint256_test_view AS
SELECT 
    id,
    value,
    value * CAST('2' AS UINT256) AS doubled_value,
    value % CAST('3' AS UINT256) AS modulus_value,
    pow(value, 2) AS squared_value
FROM iceberg.default.uint256_view_test_table
WHERE value >= CAST('200' AS UINT256);

SELECT * FROM iceberg.default.uint256_test_view ORDER BY id;