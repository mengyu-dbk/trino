-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_sorting_limit_offset_test AS
SELECT CAST(x AS UINT256) x
FROM (VALUES
    ('0'),
    ('1'),
    ('50'),
    ('100'),
    ('250'),
    ('500'),
    ('999'),
    ('1000'),
    ('9999'),
    ('99999'),
    ('999999'),
    ('9999999'),
    ('99999999'),
    ('999999999'),
    ('9999999999'),
    ('99999999999'),
    ('999999999999'),
    ('9999999999999'),
    ('99999999999999'),
    ('999999999999999')
) t(x);

SELECT x
FROM iceberg.default.uint256_sorting_limit_offset_test
ORDER BY x
OFFSET 5
LIMIT 5;