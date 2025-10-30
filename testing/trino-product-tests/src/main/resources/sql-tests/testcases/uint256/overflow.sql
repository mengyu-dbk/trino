-- database: trino; groups: uint256;
SELECT
    -- Test addition overflow
    TRY(UINT256 '115792089237316195423570985008687907853269984665640564039457584007913129639934' +
    UINT256 '1') AS add_overflow_result,
    UINT256 '115792089237316195423570985008687907853269984665640564039457584007913129639935' +
    UINT256 '0' AS max_value_plus_zero,

    -- Test subtraction underflow
    TRY(UINT256 '0' - UINT256 '1') AS sub_underflow_result,
    UINT256 '5' - UINT256 '3' AS normal_subtraction,

    -- Test multiplication overflow
    TRY(UINT256 '115792089237316195423570985008687907853269984665640564039457584007913129639935' *
    UINT256 '2') AS mul_overflow_result,
    UINT256 '100000000000000000000000000000000000000' *
    UINT256 '100000000000000000000000000000000000000' AS large_mul_result,

    -- Test bitwise shift overflow
    TRY(bitwise_left_shift(UINT256 '1', 256)) AS left_shift_overflow,
    TRY(bitwise_left_shift(UINT256 '1', 257)) AS left_shift_overflow_2,
    TRY(bitwise_right_shift(UINT256 '1', 256)) AS right_shift_overflow,
    bitwise_right_shift(UINT256 '100000000000000000000000000000000000000', 100) AS large_right_shift;