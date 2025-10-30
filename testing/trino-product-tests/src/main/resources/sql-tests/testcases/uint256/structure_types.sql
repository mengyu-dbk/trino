-- database: trino; groups: uint256;
select
  cast(row(cast('1' as uint256)) as row(x uint256)).x as row_field_access,
  array[cast(1 as uint256), cast(2 as uint256)][1] as array_element_access,
  map(array[cast(1 as uint256)], array[cast(2 as uint256)])[cast(1 as uint256)] as map_element_access