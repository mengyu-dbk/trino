-- database: trino; groups: uint256;
CREATE TABLE iceberg.default.uint256_null_test (
    id INTEGER,
    value UINT256
);

INSERT INTO iceberg.default.uint256_null_test VALUES
(1, NULL),
(2, CAST(12345 AS UINT256));

SELECT
    -- NULL checking
    id,
    value,
    value IS NULL AS is_null,
    value IS NOT NULL AS is_not_null,
    
    -- Arithmetic operations
    value + CAST(100 AS UINT256) AS add_result,
    value - CAST(50 AS UINT256) AS subtract_result,
    value * CAST(2 AS UINT256) AS multiply_result,
    value / CAST(10 AS UINT256) AS divide_result,
    value % CAST(3 AS UINT256) AS modulo_result,
    pow(value, 2) AS power_result,
    
    -- Comparison operations
    value = CAST(12345 AS UINT256) AS equal_result,
    value <> CAST(12345 AS UINT256) AS not_equal_result,
    value < CAST(20000 AS UINT256) AS less_than_result,
    value > CAST(10000 AS UINT256) AS greater_than_result,
    
    -- NULL handling functions
    COALESCE(value, CAST(0 AS UINT256)) AS coalesce_result,
    NULLIF(value, CAST(12345 AS UINT256)) AS nullif_result
FROM iceberg.default.uint256_null_test
ORDER BY id;
