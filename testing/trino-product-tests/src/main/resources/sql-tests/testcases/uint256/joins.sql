-- database: trino; groups: uint256;
WITH left_table AS (
  SELECT cast(1 as UINT256) as id, 'left1' as name
  UNION ALL
  SELECT cast(2 as UINT256) as id, 'left2' as name
),
right_table AS (
  SELECT cast(1 as UINT256) as id, 'right1' as description
  UNION ALL
  SELECT cast(3 as UINT256) as id, 'right3' as description
)
SELECT l.id, l.name, r.description
FROM left_table l
JOIN right_table r ON l.id = r.id
ORDER BY l.id