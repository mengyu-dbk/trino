-- database: trino; groups: uint256;
select
  bitwise_and(UINT256 '2', UINT256 '1') as bitwise_and,
  bitwise_or(UINT256 '2', UINT256 '1') as bitwise_or,
  bitwise_xor(UINT256 '2', UINT256 '1') as bitwise_xor,
  bitwise_not(
    UINT256 '115792089237316195423570985008687907853269984665640564039457584007913129639935'
  ) as bitwise_not,
  bit_count(UINT256 '0', 256) as bit_count,
  bitwise_left_shift(UINT256 '1', 1000) as bitwise_left_shift,
  bitwise_right_shift(UINT256 '1', 256) as bitwise_right_shift