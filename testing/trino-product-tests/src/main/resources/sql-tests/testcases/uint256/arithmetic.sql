-- database: trino; groups: uint256;
select
  -- uint256 arithmetic functions
  cast(2147483648 as uint256) + cast(2147483648 as uint256) as uint256_sum,
  cast(2147483648 as uint256) - cast(2147483648 as uint256) as uint256_subtract,
  cast(2147483648 as uint256) * cast(2 as uint256) as uint256_multiply,
  cast(2147483648 as uint256) / cast(2147483648 as uint256) as uint256_divide,
  cast(2147483648 as uint256) % cast(2 as uint256) as uint256_modulus,
  pow(uint256 '2147483648', 2) as uint256_pow