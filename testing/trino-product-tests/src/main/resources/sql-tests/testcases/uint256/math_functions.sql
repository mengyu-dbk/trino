-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

SELECT
  abs(CAST('100' AS UINT256)) AS abs_positive,
  abs(CAST('0' AS UINT256)) AS abs_zero,
  sign(CAST('100' AS UINT256)) AS sign_positive,
  sign(CAST('0' AS UINT256)) AS sign_zero,
  CAST('100' AS UINT256) % CAST('3' AS UINT256) AS mod_result,
  mod(CAST('100' AS UINT256), CAST('3' AS UINT256)) AS mod_function_result,
  greatest(CAST('100' AS UINT256), CAST('200' AS UINT256), CAST('50' AS UINT256)) AS greatest_result,
  least(CAST('100' AS UINT256), CAST('200' AS UINT256), CAST('50' AS UINT256)) AS least_result,
  pow(CAST('2' AS UINT256), CAST('10' AS UINT256)) AS pow_result,
  sqrt(CAST('100' AS UINT256)) AS sqrt_perfect_square,
  sqrt(CAST('2' AS UINT256)) AS sqrt_imperfect_square;