-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

DROP TABLE IF EXISTS iceberg.default.uint256_bucket_partitioned_test;

CREATE TABLE iceberg.default.uint256_bucket_partitioned_test (
    id UINT256,
    value VARCHAR
)
WITH (
    partitioning = ARRAY['bucket(id, 2)']
);

INSERT INTO iceberg.default.uint256_bucket_partitioned_test VALUES
(CAST('1' AS UINT256), 'first'),
(CAST('2' AS UINT256), 'second'),
(CAST('3' AS UINT256), 'third'),
(CAST('4' AS UINT256), 'fourth'),
(CAST('5' AS UINT256), 'fifth'),
(CAST('100000000000000000000000000000000000000' AS UINT256), 'large');

SELECT id, value
FROM iceberg.default.uint256_bucket_partitioned_test
ORDER BY id;