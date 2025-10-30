-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

DROP TABLE IF EXISTS iceberg.default.uint256_truncate_partitioned_test;

CREATE TABLE iceberg.default.uint256_truncate_partitioned_test (
    id UINT256,
    value VARCHAR
)
WITH (
    partitioning = ARRAY['truncate(id, 10)']
);

INSERT INTO iceberg.default.uint256_truncate_partitioned_test VALUES
(CAST('1' AS UINT256), 'first'),
(CAST('5' AS UINT256), 'fifth'),
(CAST('12' AS UINT256), 'twelfth'),
(CAST('18' AS UINT256), 'eighteenth'),
(CAST('25' AS UINT256), 'twenty-fifth'),
(CAST('100000000000000000000000000000000000015' AS UINT256), 'large');

SELECT id, value
FROM iceberg.default.uint256_truncate_partitioned_test
ORDER BY id;