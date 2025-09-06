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
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static java.lang.String.format;

public final class UInt256Operators
{
    private static final int UINT256_BYTES = 32;

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

    // CAST(varchar -> uint256) : 只支持十进制字符串转换
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
