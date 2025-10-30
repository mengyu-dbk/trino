-- database: trino; groups: uint256;
WITH test_data AS (
  SELECT cast(1 as UINT256) as id, cast(10 as UINT256) as value
  UNION ALL
  SELECT cast(2 as UINT256) as id, cast(20 as UINT256) as value
  UNION ALL
  SELECT cast(3 as UINT256) as id, cast(10 as UINT256) as value
  UNION ALL
  SELECT cast(4 as UINT256) as id, cast(30 as UINT256) as value
)
SELECT 
  id,
  value,
  ROW_NUMBER() OVER (PARTITION BY value ORDER BY id) as row_num,
  SUM(id) OVER (PARTITION BY value ORDER BY id ROWS UNBOUNDED PRECEDING) as cumulative_sum
FROM test_data
ORDER BY value, id