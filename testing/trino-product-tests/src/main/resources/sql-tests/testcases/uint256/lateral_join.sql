-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_lateral_test AS
SELECT * FROM (VALUES
    (CAST('1' AS UINT256), CAST('100' AS UINT256)),
    (CAST('2' AS UINT256), CAST('200' AS UINT256)),
    (CAST('3' AS UINT256), CAST('300' AS UINT256))
) t(id, value);

SELECT
    t.id,
    t.value,
    l.doubled_value,
    l.added_value
FROM iceberg.default.uint256_lateral_test t,
LATERAL (
    SELECT
        t.value * CAST('2' AS UINT256) AS doubled_value,
        t.value + CAST('1000' AS UINT256) AS added_value
) l
ORDER BY t.id;