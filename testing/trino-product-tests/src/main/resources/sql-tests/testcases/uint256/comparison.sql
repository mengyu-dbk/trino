-- database: trino; groups: uint256;
select 
  -- uint256 comparison
  cast(1 as UINT256) = cast(1 as UINT256) as equals_true,
  cast(1 as UINT256) = cast(2 as UINT256) as equals_false,
  cast(1 as UINT256) != cast(2 as UINT256) as different_true,
  cast(1 as UINT256) != cast(1 as UINT256) as different_false,
  cast(1 as UINT256) < cast(2 as UINT256) as less_than,
  cast(1 as UINT256) > cast(2 as UINT256) as bigger_than