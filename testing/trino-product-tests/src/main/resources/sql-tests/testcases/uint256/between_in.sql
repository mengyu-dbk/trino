-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_between_in_test AS
SELECT * FROM (VALUES
    CAST('1' AS UINT256),
    CAST('50' AS UINT256),
    CAST('100' AS UINT256),
    CAST('500' AS UINT256),
    CAST('1000' AS UINT256)
) t(x);

SELECT
    x,
    x BETWEEN CAST('10' AS UINT256) AND CAST('500' AS UINT256) AS between_10_and_500,
    x BETWEEN CAST('100' AS UINT256) AND CAST('1000' AS UINT256) AS between_100_and_1000,
    x IN (CAST('1' AS UINT256), CAST('100' AS UINT256), CAST('1000' AS UINT256)) AS in_list_1_100_1000,
    x IN (CAST('50' AS UINT256), CAST('200' AS UINT256), CAST('500' AS UINT256)) AS in_list_50_200_500
FROM iceberg.default.uint256_between_in_test
ORDER BY x;
