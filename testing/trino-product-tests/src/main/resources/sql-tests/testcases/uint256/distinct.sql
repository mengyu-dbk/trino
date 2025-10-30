-- database: trino; groups: uint256;
select 
    distinct * 
    from 
    (values (cast(1 as UINT256)), (cast(1 as UINT256))) t(col)