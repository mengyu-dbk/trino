-- database: trino; groups: uint256;
select
  -- uint256 casting
  cast(false as uint256) as uint256_casting_from_boolean,
  cast(cast(1 as integer) as uint256) as uint256_casting_from_integer,
  cast(cast(2147483648 as bigint) as uint256) as uint256_casting_from_bigint,
  cast('2147483648' as uint256) as uint256_casting_from_varchar,
  cast(cast(1 as smallint) as uint256) as uint256_casting_from_smallint,
  cast(cast(1 as tinyint) as uint256) as uint256_casting_from_tinyint,
  cast(cast(1 as decimal(38, 0)) as UINT256) as uint256_casting_from_decimal,
  cast(cast(1 as double) as UINT256) as uint256_casting_from_double,

  cast(cast(2147483648 as uint256) as double) as uint256_casting_to_double,
  cast(null as uint256) as null_to_uint256,

  cast(UINT256 '9223372036854775807' as bigint) as uint256_casting_to_bigint,
  cast(UINT256 '2147483647' as int) as uint256_casting_to_int,
  cast(UINT256 '32767' as smallint) as uint256_casting_to_smallint,
  cast(UINT256 '127' as tinyint) as uint256_casting_to_tinyint,
  cast(
    UINT256 '115792089237316195423570985008687907853269984665640564039457584007913129639935' as varchar
  ) as uint256_casting_to_varchar,
  cast(UINT256 '99999999999999999999999999999999999999' as decimal(38,0)) as uint256_casting_to_decimal