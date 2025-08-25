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

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.CAST;
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
}
