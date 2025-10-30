-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_array_agg_test AS
SELECT * FROM (VALUES
    CAST('1' AS UINT256),
    CAST('100' AS UINT256),
    CAST('1000' AS UINT256),
    CAST('1000000' AS UINT256)
) t(x);

WITH agg_arrays AS (
  SELECT 
    ARRAY_AGG(x ORDER BY x) AS array_agg_ordered,
    ARRAY_AGG(x) AS array_agg_unordered
  FROM iceberg.default.uint256_array_agg_test
)
SELECT 
  array_agg_ordered[1] AS ordered_first,
  array_agg_ordered[2] AS ordered_second,
  array_agg_ordered[3] AS ordered_third,
  array_agg_ordered[4] AS ordered_fourth,
  array_agg_unordered[1] AS unordered_first,
  array_agg_unordered[2] AS unordered_second,
  array_agg_unordered[3] AS unordered_third,
  array_agg_unordered[4] AS unordered_fourth,
  CARDINALITY(array_agg_ordered) AS ordered_length,
  CARDINALITY(array_agg_unordered) AS unordered_length
FROM agg_arrays