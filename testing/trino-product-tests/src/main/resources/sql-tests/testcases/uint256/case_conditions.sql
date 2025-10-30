-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_case_test AS
SELECT * FROM (VALUES
    CAST('0' AS UINT256),
    CAST('1' AS UINT256),
    CAST('50' AS UINT256),
    CAST('100' AS UINT256),
    CAST('1000' AS UINT256),
    CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256)
) t(x);

SELECT
    x,
    CASE
        WHEN x = CAST('0' AS UINT256) THEN 'zero'
        WHEN x = CAST('1' AS UINT256) THEN 'one'
        WHEN x < CAST('100' AS UINT256) THEN 'less than hundred'
        WHEN x >= CAST('100' AS UINT256) THEN 'hundred or more'
        ELSE 'other'
    END AS case_description,
    CASE
        WHEN x % CAST('2' AS UINT256) = CAST('0' AS UINT256) THEN true
        ELSE false
    END AS is_even,
    CASE
        WHEN x BETWEEN CAST('50' AS UINT256) AND CAST('500' AS UINT256) THEN 'in range'
        ELSE 'out of range'
    END AS range_check
FROM iceberg.default.uint256_case_test
ORDER BY x;