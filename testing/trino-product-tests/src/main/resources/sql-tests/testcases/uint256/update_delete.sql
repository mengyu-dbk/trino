-- database: trino; groups: uint256;
CREATE SCHEMA IF NOT EXISTS iceberg.default;

CREATE TABLE iceberg.default.uint256_update_delete_test (id integer, value uint256)
WITH (format_version = 2);

INSERT INTO iceberg.default.uint256_update_delete_test VALUES
(1, CAST('100' AS UINT256)),
(2, CAST('200' AS UINT256)),
(3, CAST('300' AS UINT256)),
(4, CAST('400' AS UINT256)),
(5, CAST('500' AS UINT256));

UPDATE iceberg.default.uint256_update_delete_test
SET value = value + CAST('10' AS UINT256)
WHERE id = 4;

DELETE FROM iceberg.default.uint256_update_delete_test
WHERE value < CAST('210' AS UINT256);

SELECT * FROM iceberg.default.uint256_update_delete_test ORDER BY id;