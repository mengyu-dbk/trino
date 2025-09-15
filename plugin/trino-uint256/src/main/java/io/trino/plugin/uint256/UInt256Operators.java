/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.uint256;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static java.lang.String.format;

public final class UInt256Operators
{
    private static final int UINT256_BYTES = UInt256Type.UINT256_BYTE_LENGTH;

    private UInt256Operators() {}

    // uint256 + uint256 -> uint256
    @ScalarOperator(ADD)
    @SqlType(UInt256Type.NAME)
    public static Slice add(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        byte[] out = new byte[UINT256_BYTES];

        int carry = 0;
        for (int i = UINT256_BYTES - 1; i >= 0; i--) {
            int sum = (a[i] & 0xFF) + (b[i] & 0xFF) + carry;
            out[i] = (byte) (sum & 0xFF);
            carry = (sum >>> 8) & 0xFF; // 0..1 realistically
        }
        if (carry != 0) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("uint256 addition overflow: 0x%s + 0x%s", toHex(a), toHex(b)));
        }
        return Slices.wrappedBuffer(out);
    }

    // uint256 + bigint -> uint256 (implicit conversion, overflow error)
    @ScalarOperator(ADD)
    @SqlType(UInt256Type.NAME)
    public static Slice add(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT,
                    format("Cannot add UINT256 with negative INTEGER value %s", right));
        }
        return add(left, castFromBigintToUint256(right));
    }

    // bigint + uint256 -> uint256 (implicit conversion, overflow error)
    @ScalarOperator(ADD)
    @SqlType(UInt256Type.NAME)
    public static Slice add(@SqlType(StandardTypes.BIGINT) long left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return add(castFromBigintToUint256(left), right);
    }

    // uint256 + double -> uint256 (double must be finite, non-negative integer)
    @ScalarOperator(ADD)
    @SqlType(UInt256Type.NAME)
    public static Slice add(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return add(left, castFromDoubleToUint256(right));
    }

    // double + uint256 -> uint256
    @ScalarOperator(ADD)
    @SqlType(UInt256Type.NAME)
    public static Slice add(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return add(castFromDoubleToUint256(left), right);
    }

    // uint256 - uint256 -> uint256 (underflow error)
    @ScalarOperator(SUBTRACT)
    @SqlType(UInt256Type.NAME)
    public static Slice subtract(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        // BigInteger handles unsigned arithmetic if we use positive sign
        BigInteger biA = new BigInteger(1, a);
        BigInteger biB = new BigInteger(1, b);
        BigInteger res = biA.subtract(biB);
        if (res.signum() < 0) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("uint256 subtraction underflow: 0x%s - 0x%s", toHex(a), toHex(b)));
        }
        return Slices.wrappedBuffer(toFixedUint256(res));
    }

    // uint256 - bigint -> uint256 (implicit conversion, underflow error)
    @ScalarOperator(SUBTRACT)
    @SqlType(UInt256Type.NAME)
    public static Slice subtract(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT,
                    format("Cannot subtract UINT256 with negative INTEGER value %s", right));
        }
        return subtract(left, castFromBigintToUint256(right));
    }

    // bigint - uint256 -> uint256 (implicit conversion, underflow/invalid cast error)
    @ScalarOperator(SUBTRACT)
    @SqlType(UInt256Type.NAME)
    public static Slice subtract(@SqlType(StandardTypes.BIGINT) long left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return subtract(castFromBigintToUint256(left), right);
    }

    // uint256 - double -> uint256 (double must be finite, non-negative integer)
    @ScalarOperator(SUBTRACT)
    @SqlType(UInt256Type.NAME)
    public static Slice subtract(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return subtract(left, castFromDoubleToUint256(right));
    }

    // double - uint256 -> uint256 (double must be finite, non-negative integer)
    @ScalarOperator(SUBTRACT)
    @SqlType(UInt256Type.NAME)
    public static Slice subtract(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return subtract(castFromDoubleToUint256(left), right);
    }

    // uint256 * uint256 -> uint256 (overflow error)
    @ScalarOperator(MULTIPLY)
    @SqlType(UInt256Type.NAME)
    public static Slice multiply(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        BigInteger biA = new BigInteger(1, a);
        BigInteger biB = new BigInteger(1, b);
        BigInteger res = biA.multiply(biB);
        if (res.bitLength() > 256) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("uint256 multiplication overflow: 0x%s * 0x%s", toHex(a), toHex(b)));
        }
        return Slices.wrappedBuffer(toFixedUint256(res));
    }

    // uint256 * integer -> uint256 (implicit conversion, overflow error)
    @ScalarOperator(MULTIPLY)
    @SqlType(UInt256Type.NAME)
    public static Slice multiply(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT,
                format("Cannot multiply UINT256 with negative INTEGER value %s", right));
        }
        return multiply(left, castFromBigintToUint256(right));
    }

    // integer * uint256 -> uint256 (implicit conversion, overflow error)
    @ScalarOperator(MULTIPLY)
    @SqlType(UInt256Type.NAME)
    public static Slice multiply(@SqlType(StandardTypes.BIGINT) long left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return multiply(right, left);
    }

    // uint256 * double -> uint256 (double must be finite, non-negative integer)
    @ScalarOperator(MULTIPLY)
    @SqlType(UInt256Type.NAME)
    public static Slice multiply(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return multiply(left, castFromDoubleToUint256(right));
    }

    // double * uint256 -> uint256
    @ScalarOperator(MULTIPLY)
    @SqlType(UInt256Type.NAME)
    public static Slice multiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return multiply(castFromDoubleToUint256(left), right);
    }

    // uint256 / uint256 -> uint256 (division by zero error)
    @ScalarOperator(DIVIDE)
    @SqlType(UInt256Type.NAME)
    public static Slice divide(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        BigInteger biA = new BigInteger(1, a);
        BigInteger biB = new BigInteger(1, b);
        if (biB.signum() == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        BigInteger res = biA.divide(biB);
        return Slices.wrappedBuffer(toFixedUint256(res));
    }

    // uint256 / bigint -> uint256 (implicit conversion, division by zero)
    @ScalarOperator(DIVIDE)
    @SqlType(UInt256Type.NAME)
    public static Slice divide(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT,
                    format("Cannot divide UINT256 with negative INTEGER value %s", right));
        }
        return divide(left, castFromBigintToUint256(right));
    }

    // bigint / uint256 -> uint256 (implicit conversion, division by zero)
    @ScalarOperator(DIVIDE)
    @SqlType(UInt256Type.NAME)
    public static Slice divide(@SqlType(StandardTypes.BIGINT) long left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return divide(castFromBigintToUint256(left), right);
    }

    // uint256 / double -> uint256 (double must be finite, non-negative integer; division by zero handled in divide)
    @ScalarOperator(DIVIDE)
    @SqlType(UInt256Type.NAME)
    public static Slice divide(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return divide(left, castFromDoubleToUint256(right));
    }

    // double / uint256 -> uint256 (double must be finite, non-negative integer)
    @ScalarOperator(DIVIDE)
    @SqlType(UInt256Type.NAME)
    public static Slice divide(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return divide(castFromDoubleToUint256(left), right);
    }

    // uint256 % uint256 -> uint256 (modulus by zero error)
    @ScalarOperator(MODULUS)
    @SqlType(UInt256Type.NAME)
    public static Slice modulus(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        BigInteger biA = new BigInteger(1, a);
        BigInteger biB = new BigInteger(1, b);
        if (biB.signum() == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        BigInteger res = biA.remainder(biB);
        return Slices.wrappedBuffer(toFixedUint256(res));
    }

    // uint256 % bigint -> uint256 (implicit conversion, modulus by zero)
    @ScalarOperator(MODULUS)
    @SqlType(UInt256Type.NAME)
    public static Slice modulus(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.BIGINT) long right)
    {
        if (right < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT,
                    format("Cannot modulus UINT256 with negative INTEGER value %s", right));
        }
        return modulus(left, castFromBigintToUint256(right));
    }

    // bigint % uint256 -> uint256 (implicit conversion, modulus by zero)
    @ScalarOperator(MODULUS)
    @SqlType(UInt256Type.NAME)
    public static Slice modulus(@SqlType(StandardTypes.BIGINT) long left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return modulus(castFromBigintToUint256(left), right);
    }

    // uint256 % double -> uint256 (double must be finite, non-negative integer)
    @ScalarOperator(MODULUS)
    @SqlType(UInt256Type.NAME)
    public static Slice modulus(@SqlType(UInt256Type.NAME) Slice left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return modulus(left, castFromDoubleToUint256(right));
    }

    // double % uint256 -> uint256 (double must be finite, non-negative integer)
    @ScalarOperator(MODULUS)
    @SqlType(UInt256Type.NAME)
    public static Slice modulus(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(UInt256Type.NAME) Slice right)
    {
        return modulus(castFromDoubleToUint256(left), right);
    }

    /*
        Trino 不支持自定义运算符函数覆盖内置的实现
        io.trino.metadata.GlobalFunctionCatalog.checkNotSpecializedTypeOperator
        以下是不支持的列表
            1. EQUAL
            2. IDENTICAL
            3. INDETERMINATE
            4. HASH_CODE
            5. XX_HASH_64
            6. COMPARISON_UNORDERED_FIRST
            7. COMPARISON_UNORDERED_LAST
            8. LESS_THAN
            9. LESS_THAN_OR_EQUAL
        因此以下的重载实现均无效
   */

    // CAST(varbinary -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromVarbinaryToUint256(@SqlType("varbinary") Slice input)
    {
        int len = input.length();
        if (len > UINT256_BYTES) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Invalid UINT256 binary length: %s (max %s)", len, UINT256_BYTES));
        }
        // left-pad with zeros to 32 bytes, big-endian representation
        if (len == UINT256_BYTES) {
            return input;
        }
        byte[] bytes = new byte[UINT256_BYTES];
        // copy to the least significant end (right aligned)
        input.getBytes(0, bytes, UINT256_BYTES - len, len);
        return Slices.wrappedBuffer(bytes);
    }

    // CAST(uint256 -> varbinary)
    @ScalarOperator(CAST)
    @SqlType("varbinary")
    public static Slice castFromUint256ToVarbinary(@SqlType(UInt256Type.NAME) Slice value)
    {
        // Always 32 bytes normalized
        return Slices.wrappedBuffer(ensureUint256(value));
    }

    // CAST(bigint -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromBigintToUint256(@SqlType(StandardTypes.BIGINT) long input)
    {
        if (input < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative BIGINT value %s to UINT256", input));
        }
        return uint256(input);
    }

    // CAST(integer -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromIntegerToUint256(@SqlType(StandardTypes.INTEGER) long input)
    {
        if (input < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative INTEGER value %s to UINT256", input));
        }
        return uint256(input);
    }

    // CAST(smallint -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromSmallintToUint256(@SqlType(StandardTypes.SMALLINT) long input)
    {
        if (input < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative SMALLINT value %s to UINT256", input));
        }
        return uint256(input);
    }

    // CAST(tinyint -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromTinyintToUint256(@SqlType(StandardTypes.TINYINT) long input)
    {
        if (input < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative TINYINT value %s to UINT256", input));
        }
        return uint256(input);
    }

    // CAST(real -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromRealToUint256(@SqlType(StandardTypes.REAL) long input)
    {
        float value = Float.intBitsToFloat((int) input);
        if (value < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative REAL value %s to UINT256", value));
        }
        if (!Float.isFinite(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast non-finite REAL value %s to UINT256", value));
        }
        if (value != Math.floor(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast non-integer REAL value %s to UINT256", value));
        }
        // Convert to BigInteger to handle large values
        BigInteger bigValue = new BigInteger(String.valueOf((long) value));
        return Slices.wrappedBuffer(toFixedUint256(bigValue));
    }

    // CAST(double -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromDoubleToUint256(@SqlType(StandardTypes.DOUBLE) double input)
    {
        if (input < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative DOUBLE value %s to UINT256", input));
        }
        if (!Double.isFinite(input)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast non-finite DOUBLE value %s to UINT256", input));
        }
        if (input != Math.floor(input)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast non-integer DOUBLE value %s to UINT256", input));
        }
        // Use BigDecimal for precision when converting large doubles
        java.math.BigDecimal decimal = java.math.BigDecimal.valueOf(input);
        BigInteger bigValue = decimal.toBigInteger();
        return Slices.wrappedBuffer(toFixedUint256(bigValue));
    }

    // CAST(short decimal -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromShortDecimalToUint256(@SqlType("decimal") long input)
    {
        // Convert short decimal to BigInteger
        BigInteger decimalValue = BigInteger.valueOf(input);
        return Slices.wrappedBuffer(toFixedUint256(decimalValue));
    }

    // CAST(long decimal -> uint256)
    @ScalarOperator(CAST)
    @LiteralParameters({"p", "s"})
    @SqlType(UInt256Type.NAME)
    public static Slice castFromLongDecimalToUint256(@LiteralParameter("p") long precision, @LiteralParameter("s") long scale, @SqlType("decimal(p,s)") Int128 input)
    {
        // Convert long decimal to BigInteger
        BigInteger decimalValue = input.toBigInteger();

        // Check if the decimal has non-zero fractional part
        if (scale > 0) {
            // For decimal with scale > 0, we need to check if the fractional part is zero
            // This is done by checking if the value is divisible by 10^scale
            BigInteger scaleFactor = BigInteger.TEN.pow((int) scale);
            if (!decimalValue.remainder(scaleFactor).equals(BigInteger.ZERO)) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast non-integer DECIMAL value to UINT256"));
            }
            // Remove the scale factor to get the integer part
            decimalValue = decimalValue.divide(scaleFactor);
        }

        // Check if the value is negative
        if (decimalValue.signum() < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative DECIMAL value to UINT256"));
        }

        // Check if the value fits in uint256
        if (decimalValue.bitLength() > 256) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("DECIMAL value too large for UINT256"));
        }

        return Slices.wrappedBuffer(toFixedUint256(decimalValue));
    }

    // CAST(varchar -> uint256) : 只支持十进制字符��转换
    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(UInt256Type.NAME)
    public static Slice castFromVarcharToUint256(@SqlType("varchar(x)") Slice input)
    {
        byte[] bytes = input.getBytes();
        String res = new String(bytes, StandardCharsets.UTF_8);
        try {
            return Slices.wrappedBuffer(toFixedUint256(new BigInteger(res, 10)));
        }
        catch (NumberFormatException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Invalid UINT256 value: %s", res));
        }
    }

    // CAST(uint256 -> varchar) : 输出十进制字符串
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromUint256ToVarchar(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        return Slices.wrappedBuffer(new BigInteger(1, bytes).toString(10).getBytes(StandardCharsets.UTF_8));
    }

    // CAST(uint256 -> long decimal)
    @ScalarOperator(CAST)
    @LiteralParameters({"p", "s"})
    @SqlType("decimal(p,s)")
    public static Int128 castFromUint256ToLongDecimal(@LiteralParameter("p") long precision, @LiteralParameter("s") long scale, @SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        BigInteger uint256Value = new BigInteger(1, bytes);

        // Check if the value fits in the specified decimal precision
        if (uint256Value.toString().length() > precision - scale) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("UINT256 value too large for DECIMAL(%d,%d)", precision, scale));
        }

        uint256Value = uint256Value.multiply(BigInteger.valueOf(10).pow((int) scale));
        return Int128.valueOf(uint256Value);
    }

    // Convenience constructor function: uint256(varbinary)
    @ScalarFunction(UInt256Type.NAME)
    @SqlType(UInt256Type.NAME)
    public static Slice uint256(@SqlType("varbinary") Slice input)
    {
        return castFromVarbinaryToUint256(input);
    }

    // Convenience constructor function: uint256(bigint)
    @ScalarFunction(UInt256Type.NAME)
    @SqlType(UInt256Type.NAME)
    public static Slice uint256(@SqlType(StandardTypes.BIGINT) long input)
    {
        if (input < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative BIGINT value %s to UINT256", input));
        }
        return castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, // 最高24字节为空
                (byte) ((input >>> 56) & 0xFF),
                (byte) ((input >>> 48) & 0xFF),
                (byte) ((input >>> 40) & 0xFF),
                (byte) ((input >>> 32) & 0xFF),
                (byte) ((input >>> 24) & 0xFF),
                (byte) ((input >>> 16) & 0xFF),
                (byte) ((input >>> 8) & 0xFF),
                (byte) (input & 0xFF)
        }));
    }

    // CAST(uint256 -> bigint)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castFromUint256ToBigint(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, bytes);

        // Check if the value fits in a long
        if (bigValue.bitLength() > 63) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("UINT256 value too large for BIGINT: %s", bigValue.toString()));
        }

        return bigValue.longValue();
    }

    // CAST(uint256 -> integer)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castFromUint256ToInteger(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, bytes);

        // Check if the value fits in an integer (32-bit signed)
        if (bigValue.bitLength() > 31) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("UINT256 value too large for INTEGER: %s", bigValue.toString()));
        }

        return bigValue.intValue();
    }

    // CAST(uint256 -> smallint)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castFromUint256ToSmallint(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, bytes);

        // Check if the value fits in a smallint (16-bit signed)
        if (bigValue.bitLength() > 15) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("UINT256 value too large for SMALLINT: %s", bigValue.toString()));
        }

        return bigValue.shortValue();
    }

    // CAST(uint256 -> tinyint)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castFromUint256ToTinyint(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, bytes);

        // Check if the value fits in a tinyint (8-bit signed)
        if (bigValue.bitLength() > 7) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("UINT256 value too large for TINYINT: %s", bigValue.toString()));
        }

        return bigValue.byteValue();
    }

    // CAST(uint256 -> real)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castFromUint256ToReal(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, bytes);

        // Check if the value fits in a float
        if (bigValue.bitLength() > 24) { // float has 24 bits of precision
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("UINT256 value too large for REAL: %s", bigValue.toString()));
        }

        float floatValue = bigValue.floatValue();
        return Float.floatToIntBits(floatValue);
    }

    // CAST(uint256 -> double)
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castFromUint256ToDouble(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, bytes);

        // Check if the value fits in a double
        if (bigValue.bitLength() > 53) { // double has 53 bits of precision
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("UINT256 value too large for DOUBLE: %s", bigValue.toString()));
        }

        return bigValue.doubleValue();
    }

    // CAST(boolean -> uint256)
    @ScalarOperator(CAST)
    @SqlType(UInt256Type.NAME)
    public static Slice castFromBooleanToUint256(@SqlType(StandardTypes.BOOLEAN) boolean input)
    {
        // false -> 0, true -> 1
        return input ? uint256(1L) : uint256(0L);
    }

    // 位运算：与
    @ScalarFunction("bitwise_and")
    @SqlType(UInt256Type.NAME)
    public static Slice bitwiseAnd(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        byte[] out = new byte[UINT256_BYTES];
        for (int i = 0; i < UINT256_BYTES; i++) {
            out[i] = (byte) (a[i] & b[i]);
        }
        return Slices.wrappedBuffer(out);
    }

    // 位运算：或
    @ScalarFunction("bitwise_or")
    @SqlType(UInt256Type.NAME)
    public static Slice bitwiseOr(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        byte[] out = new byte[UINT256_BYTES];
        for (int i = 0; i < UINT256_BYTES; i++) {
            out[i] = (byte) (a[i] | b[i]);
        }
        return Slices.wrappedBuffer(out);
    }

    // 位运算：异或
    @ScalarFunction("bitwise_xor")
    @SqlType(UInt256Type.NAME)
    public static Slice bitwiseXor(@SqlType(UInt256Type.NAME) Slice left, @SqlType(UInt256Type.NAME) Slice right)
    {
        byte[] a = ensureUint256(left);
        byte[] b = ensureUint256(right);
        byte[] out = new byte[UINT256_BYTES];
        for (int i = 0; i < UINT256_BYTES; i++) {
            out[i] = (byte) (a[i] ^ b[i]);
        }
        return Slices.wrappedBuffer(out);
    }

    // 位运算：按位取反
    @ScalarFunction("bitwise_not")
    @SqlType(UInt256Type.NAME)
    public static Slice bitwiseNot(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] a = ensureUint256(value);
        byte[] out = new byte[UINT256_BYTES];
        for (int i = 0; i < UINT256_BYTES; i++) {
            out[i] = (byte) (~a[i]);
        }
        return Slices.wrappedBuffer(out);
    }

    // 位运算：左移
    @ScalarFunction("bitwise_left_shift")
    @SqlType(UInt256Type.NAME)
    public static Slice bitwiseLeftShift(@SqlType(UInt256Type.NAME) Slice value, @SqlType(StandardTypes.BIGINT) long shiftBits)
    {
        if (shiftBits < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Shift amount cannot be negative");
        }
        if (shiftBits >= 256) {
            // 移位超过256位，结果为0
            return Slices.wrappedBuffer(new byte[UINT256_BYTES]);
        }

        byte[] input = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, input);
        BigInteger result = bigValue.shiftLeft((int) shiftBits);

        // 检查溢出
        if (result.bitLength() > 256) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Left shift overflow: value would exceed 256 bits"));
        }

        return Slices.wrappedBuffer(toFixedUint256(result));
    }

    // 位运算：右移
    @ScalarFunction("bitwise_right_shift")
    @SqlType(UInt256Type.NAME)
    public static Slice bitwiseRightShift(@SqlType(UInt256Type.NAME) Slice value, @SqlType(StandardTypes.BIGINT) long shiftBits)
    {
        if (shiftBits < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Shift amount cannot be negative");
        }
        if (shiftBits >= 256) {
            // 移位超过256位，结果为0
            return Slices.wrappedBuffer(new byte[UINT256_BYTES]);
        }

        byte[] input = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, input);
        BigInteger result = bigValue.shiftRight((int) shiftBits);

        return Slices.wrappedBuffer(toFixedUint256(result));
    }

    // bit_count: 计算位中1的个数
    @ScalarFunction("bit_count")
    @SqlType(StandardTypes.BIGINT)
    public static long bitCount(@SqlType(UInt256Type.NAME) Slice value)
    {
        byte[] bytes = ensureUint256(value);
        long count = 0;
        for (byte b : bytes) {
            count += Integer.bitCount(b & 0xFF);
        }
        return count;
    }

    // bit_count: 计算指定位数范围内1的个数
    @ScalarFunction("bit_count")
    @SqlType(StandardTypes.BIGINT)
    public static long bitCount(@SqlType(UInt256Type.NAME) Slice value, @SqlType(StandardTypes.BIGINT) long bits)
    {
        if (bits <= 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "bits must be positive");
        }
        if (bits > 256) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "bits cannot exceed 256 for UINT256");
        }

        byte[] bytes = ensureUint256(value);
        BigInteger bigValue = new BigInteger(1, bytes);

        // 创建一个掩码来限制位数
        BigInteger mask;
        if (bits >= 256) {
            // 如果bits >= 256，就使用完整的值
            mask = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
        }
        else {
            mask = BigInteger.ONE.shiftLeft((int) bits).subtract(BigInteger.ONE);
        }

        // 应用掩码
        BigInteger maskedValue = bigValue.and(mask);

        // 计算1的个数
        return maskedValue.bitCount();
    }

    // pow: uint256^bigint -> uint256
    @ScalarFunction("pow")
    @SqlType(UInt256Type.NAME)
    public static Slice pow(@SqlType(UInt256Type.NAME) Slice base, @SqlType(StandardTypes.BIGINT) long exponent)
    {
        if (exponent < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT,
                format("Cannot raise UINT256 to negative power: %s", exponent));
        }

        BigInteger baseBig = getBigInteger(base);
        BigInteger expBig = BigInteger.valueOf(exponent);

        // 特殊情况处理
        if (exponent == 0) {
            return Slices.wrappedBuffer(toFixedUint256(BigInteger.ONE));
        }

        if (baseBig.equals(BigInteger.ZERO)) {
            return Slices.wrappedBuffer(new byte[UINT256_BYTES]);
        }

        if (baseBig.equals(BigInteger.ONE)) {
            return Slices.wrappedBuffer(toFixedUint256(BigInteger.ONE));
        }

        // 使用快速幂算法计算
        BigInteger result = powMod(baseBig, expBig, null);

        // 检查结果是否超出uint256范围
        if (result.bitLength() > 256) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                format("pow overflow: result exceeds uint256 maximum value"));
        }

        return Slices.wrappedBuffer(toFixedUint256(result));
    }

    /**
     * 快速幂算法实现，支持模运算优化
     * 如果modulus为null，则进行普通的幂运算
     */
    private static BigInteger powMod(BigInteger base, BigInteger exponent, BigInteger modulus)
    {
        if (exponent.equals(BigInteger.ZERO)) {
            return BigInteger.ONE;
        }

        BigInteger result = BigInteger.ONE;
        BigInteger currentBase = base;
        BigInteger exp = exponent;

        // 使用二进制幂运算算法
        while (exp.compareTo(BigInteger.ZERO) > 0) {
            // 如果指数是奇数，将当前底数乘到结果中
            if (exp.testBit(0)) {
                result = result.multiply(currentBase);
                if (modulus != null) {
                    result = result.mod(modulus);
                }
                // 早期溢出检查，避免计算过大的中间结果
                else if (result.bitLength() > 256) {
                    throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                        "pow overflow: intermediate result exceeds uint256 maximum value");
                }
            }

            // 将底数平方，指数除以2
            currentBase = currentBase.multiply(currentBase);
            if (modulus != null) {
                currentBase = currentBase.mod(modulus);
            }
            // 早期溢出检查
            else if (currentBase.bitLength() > 256 && exp.compareTo(BigInteger.ONE) > 0) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
                    "pow overflow: intermediate result exceeds uint256 maximum value");
            }

            exp = exp.shiftRight(1); // exp /= 2
        }

        return result;
    }

    private static byte[] ensureUint256(Slice value) // 保证是32字节，不足左侧补0
    {
        int len = value.length();
        if (len == UINT256_BYTES) {
            return value.getBytes();
        }
        if (len > UINT256_BYTES) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Invalid UINT256 binary length: %s (max %s)", len, UINT256_BYTES));
        }
        byte[] out = new byte[UINT256_BYTES];
        value.getBytes(0, out, UINT256_BYTES - len, len);
        return out;
    }

    public static BigInteger getBigInteger(Slice value)
    {
        byte[] bytes = ensureUint256(value);
        return new BigInteger(1, bytes);
    }

    public static long getLong(Slice value)
    {
        byte[] bytes = ensureUint256(value);
        return (new BigInteger(1, bytes)).longValue();
    }

    private static String toHex(byte[] bytes)
    {
        char[] h = "0123456789abcdef".toCharArray();
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            out[i * 2] = h[v >>> 4];
            out[i * 2 + 1] = h[v & 0x0F];
        }
        return new String(out);
    }

    private static byte[] toFixedUint256(BigInteger value)
    {
        if (value.signum() < 0 || value.bitLength() > 256) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "uint256 value out of range");
        }
        byte[] tmp = value.toByteArray(); // big-endian, may contain leading zero
        if (tmp.length == 0) {
            return new byte[UINT256_BYTES];
        }
        // strip possible leading sign byte 0x00
        int offset = 0;
        if (tmp.length > 1 && tmp[0] == 0) {
            offset = 1;
        }
        int len = tmp.length - offset;
        if (len > UINT256_BYTES) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "uint256 value out of range");
        }
        byte[] out = new byte[UINT256_BYTES];
        System.arraycopy(tmp, offset, out, UINT256_BYTES - len, len);
        return out;
    }
}
