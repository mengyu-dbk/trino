-- database: trino; groups: uint256;
WITH test_data AS (
  SELECT cast(1 as UINT256) as id, cast(10 as UINT256) as value
  UNION ALL
  SELECT cast(2 as UINT256) as id, cast(20 as UINT256) as value
  UNION ALL
  SELECT cast(3 as UINT256) as id, cast(10 as UINT256) as value
)
SELECT id, value
FROM test_data
WHERE id > cast(1 as UINT256)
GROUP BY id, value
ORDER BY value DESC, id ASC