-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_aggregation_test AS
SELECT * FROM (VALUES
    CAST('1000000' AS UINT256),
    CAST('1' AS UINT256),
    CAST('1' AS UINT256),
    CAST('100' AS UINT256)
) t(x);

SELECT
    MAX(x) AS max,
    MIN(x) AS min,
    AVG(x) AS avg,
    SUM(x) AS sum,
    COUNT(DISTINCT x) AS count_distinct,
    APPROX_DISTINCT(x) AS approx_distinct,
    ARBITRARY(x) AS arbitrary,
    STDDEV_POP(CAST(x AS DOUBLE)) AS stddev_pop,
    STDDEV_SAMP(CAST(x AS DOUBLE)) AS stddev_samp,
    VAR_POP(CAST(x AS DOUBLE)) AS var_pop,
    VAR_SAMP(CAST(x AS DOUBLE)) AS var_samp,
    BITWISE_OR_AGG(x) AS bitwise_or_agg,
    BITWISE_AND_AGG(x) AS bitwise_and_agg
FROM iceberg.default.uint256_aggregation_test;