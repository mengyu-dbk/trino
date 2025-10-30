-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

SELECT
  -- Basic valid cases
  CAST('0' AS UINT256) AS zero,
  CAST('1' AS UINT256) AS one,
  CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935' AS UINT256) AS max_value,

  -- Leading zeros
  CAST('0000' AS UINT256) AS zero_with_leading_zeros,
  CAST('0001' AS UINT256) AS one_with_leading_zeros,
  CAST('0123' AS UINT256) AS number_with_leading_zeros,

  -- Whitespace handling
  CAST(' 123 ' AS UINT256) AS number_with_spaces,
  CAST(' 123' AS UINT256) AS number_with_leading_space,
  CAST('123 ' AS UINT256) AS number_with_trailing_space,

  -- Error cases with TRY
  TRY(CAST('' AS UINT256)) AS empty_string,
  TRY(CAST(' ' AS UINT256)) AS whitespace_only,
  TRY(CAST('-1' AS UINT256)) AS negative_number,
  TRY(CAST('1a' AS UINT256)) AS alphanumeric,
  TRY(CAST('abc' AS UINT256)) AS letters_only,
  TRY(CAST('1.5' AS UINT256)) AS decimal_number,
  TRY(CAST('1e10' AS UINT256)) AS scientific_notation,
  TRY(CAST('115792089237316195423570985008687907853269984665640564039457584007913129639936' AS UINT256)) AS overflow_value
;