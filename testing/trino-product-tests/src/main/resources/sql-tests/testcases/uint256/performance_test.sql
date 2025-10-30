-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_performance_test (id integer, value uint256)
WITH (format_version = 2);

INSERT INTO iceberg.default.uint256_performance_test 
SELECT 
  i,
  CAST(CAST(1000000000 AS UINT256) + CAST(i AS UINT256) AS UINT256) 
FROM UNNEST(sequence(1, 10000)) t(i);

SELECT COUNT(*) FROM iceberg.default.uint256_performance_test;
