-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

WITH distinct_array AS (
  SELECT 
    ARRAY_DISTINCT(ARRAY[
      CAST('1' AS UINT256), 
      CAST('100' AS UINT256), 
      CAST('1' AS UINT256), 
      CAST('1000' AS UINT256), 
      CAST('100' AS UINT256)
    ]) AS array_with_duplicates_removed
)
SELECT 
  array_with_duplicates_removed[1] AS first_element,
  array_with_duplicates_removed[2] AS second_element,
  array_with_duplicates_removed[3] AS third_element,
  CARDINALITY(array_with_duplicates_removed) AS array_length
FROM distinct_array