-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

DROP TABLE IF EXISTS iceberg.default.uint256_subquery_test;
DROP TABLE IF EXISTS iceberg.default.uint256_subquery_lookup;

CREATE TABLE iceberg.default.uint256_subquery_test AS
SELECT * FROM (VALUES
    (CAST('0' AS UINT256), CAST('100' AS UINT256)),
    (CAST('1' AS UINT256), CAST('200' AS UINT256)),
    (CAST('2' AS UINT256), CAST('300' AS UINT256)),
    (CAST('3' AS UINT256), CAST('100' AS UINT256))
) t(id, value);

CREATE TABLE iceberg.default.uint256_subquery_lookup AS
SELECT * FROM (VALUES
    (CAST('100' AS UINT256), 'category_a'),
    (CAST('200' AS UINT256), 'category_b')
) t(lookup_value, category);

SELECT
  id,
  value,
  (SELECT MAX(lookup_value) FROM iceberg.default.uint256_subquery_lookup) AS max_lookup_value
FROM iceberg.default.uint256_subquery_test
ORDER BY id;