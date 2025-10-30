-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

DROP TABLE IF EXISTS iceberg.default.uint256_multi_partitioned_test;

CREATE TABLE iceberg.default.uint256_multi_partitioned_test (
    id UINT256,
    value VARCHAR,
    category UINT256,
    subcategory UINT256
)
WITH (
    partitioning = ARRAY['category', 'subcategory']
);

INSERT INTO iceberg.default.uint256_multi_partitioned_test VALUES
(CAST('1' AS UINT256), 'first', CAST('100' AS UINT256), CAST('10' AS UINT256)),
(CAST('2' AS UINT256), 'second', CAST('100' AS UINT256), CAST('20' AS UINT256)),
(CAST('3' AS UINT256), 'third', CAST('200' AS UINT256), CAST('10' AS UINT256)),
(CAST('4' AS UINT256), 'fourth', CAST('200' AS UINT256), CAST('20' AS UINT256)),
(CAST('5' AS UINT256), 'fifth', CAST('300' AS UINT256), CAST('10' AS UINT256)),
(CAST('6' AS UINT256), 'sixth', CAST('100' AS UINT256), CAST('10' AS UINT256));

SELECT category, subcategory, COUNT(*) as count
FROM iceberg.default.uint256_multi_partitioned_test
GROUP BY category, subcategory
ORDER BY category, subcategory;