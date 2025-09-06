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
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.math.BigInteger;

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
    @SqlType("uint256")
    public static Slice add(@SqlType("uint256") Slice left, @SqlType("uint256") Slice right)
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
    @SqlType("uint256")
    public static Slice subtract(@SqlType("uint256") Slice left, @SqlType("uint256") Slice right)
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
    @SqlType("uint256")
    public static Slice multiply(@SqlType("uint256") Slice left, @SqlType("uint256") Slice right)
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
    @SqlType("uint256")
    public static Slice divide(@SqlType("uint256") Slice left, @SqlType("uint256") Slice right)
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
    @SqlType("uint256")
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
    public static Slice castFromUint256ToVarbinary(@SqlType("uint256") Slice value)
    {
        // Always 32 bytes normalized
        return Slices.wrappedBuffer(ensureUint256(value));
    }

    // CAST(bigint -> uint256)
    @ScalarOperator(CAST)
    @SqlType("uint256")
    public static Slice castFromBigintToUint256(@SqlType(StandardTypes.BIGINT) long input)
    {
        if (input < 0) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast negative BIGINT value %s to UINT256", input));
        }
        return uint256(input);
    }
    /*
    // CAST(varchar -> uint256) : 支持可选 0x 前缀的十六进制字符串（大小写均可），长度<=64，奇数字符长度会自动左侧补0
    @ScalarOperator(CAST)
    @SqlType("uint256")
    public static Slice castFromVarcharToUint256(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        String s = input.toStringUtf8().trim();
        if (s.startsWith("0x") || s.startsWith("0X")) {
            s = s.substring(2);
        }
        if (s.isEmpty()) {
            // treat empty as zero
            return Slices.wrappedBuffer(new byte[UINT256_BYTES]);
        }
        // validate hex characters
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!isHex) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Invalid hex digit '%s' in VARCHAR for UINT256", c));
            }
        }
        if (s.length() > 64) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Invalid UINT256 hex length: %s (max 64)", s.length()));
        }
        if ((s.length() & 1) == 1) {
            s = "0" + s;
        }
        byte[] decoded = hexToBytes(s);
        // left-pad to 32 bytes
        if (decoded.length == UINT256_BYTES) {
            return Slices.wrappedBuffer(decoded);
        }
        byte[] out = new byte[UINT256_BYTES];
        System.arraycopy(decoded, 0, out, UINT256_BYTES - decoded.length, decoded.length);
        return Slices.wrappedBuffer(out);
    }

    // CAST(uint256 -> varchar) : 输出64位小写十六进制字符串（无0x前缀）
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromUint256ToVarchar(@SqlType("uint256") Slice value)
    {
        byte[] bytes = ensureUint256(value);
        String hex = toHex(bytes);
        return Slices.wrappedBuffer(hex.getBytes(StandardCharsets.UTF_8));
    }
    */
    // Convenience constructor function: uint256(varbinary)
    @ScalarFunction("uint256")
    @SqlType("uint256")
    public static Slice uint256(@SqlType("varbinary") Slice input)
    {
        return castFromVarbinaryToUint256(input);
    }

    // Convenience constructor function: uint256(bigint)
    @ScalarFunction("uint256")
    @SqlType("uint256")
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
    @SqlType("uint256")
    public static Slice bitwiseAnd(@SqlType("uint256") Slice left, @SqlType("uint256") Slice right)
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
    @SqlType("uint256")
    public static Slice bitwiseOr(@SqlType("uint256") Slice left, @SqlType("uint256") Slice right)
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
    @SqlType("uint256")
    public static Slice bitwiseXor(@SqlType("uint256") Slice left, @SqlType("uint256") Slice right)
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
    @SqlType("uint256")
    public static Slice bitwiseNot(@SqlType("uint256") Slice value)
    {
        byte[] a = ensureUint256(value);
        byte[] out = new byte[UINT256_BYTES];
        for (int i = 0; i < UINT256_BYTES; i++) {
            out[i] = (byte) (~a[i]);
        }
        return Slices.wrappedBuffer(out);
    }

    private static byte[] ensureUint256(Slice value)
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

    private static byte[] hexToBytes(String hex)
    {
        int len = hex.length();
        byte[] out = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            int hi = Character.digit(hex.charAt(i), 16);
            int lo = Character.digit(hex.charAt(i + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, "Invalid hex string for UINT256");
            }
            out[i / 2] = (byte) ((hi << 4) | lo);
        }
        return out;
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
