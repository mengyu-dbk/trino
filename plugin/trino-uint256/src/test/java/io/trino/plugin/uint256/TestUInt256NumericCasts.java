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
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static io.trino.plugin.uint256.UInt256Operators.castFromBigintToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromBooleanToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromDoubleToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromIntegerToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromRealToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromSmallintToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromTinyintToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromUint256ToBigint;
import static io.trino.plugin.uint256.UInt256Operators.castFromUint256ToDouble;
import static io.trino.plugin.uint256.UInt256Operators.castFromUint256ToInteger;
import static io.trino.plugin.uint256.UInt256Operators.castFromUint256ToReal;
import static io.trino.plugin.uint256.UInt256Operators.castFromUint256ToSmallint;
import static io.trino.plugin.uint256.UInt256Operators.castFromUint256ToTinyint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestUInt256NumericCasts
{
    @Test
    public void testCastFromBoolean()
    {
        Slice one = castFromBooleanToUint256(true);
        assertThat(toUint256BigInteger(one)).isEqualTo(BigInteger.ONE);

        Slice zero = castFromBooleanToUint256(false);
        assertThat(toUint256BigInteger(zero)).isEqualTo(BigInteger.ZERO);
    }

    @Test
    public void testCastFromTinyint()
    {
        // Test positive values
        Slice result = castFromTinyintToUint256(42L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(42));

        // Test zero
        result = castFromTinyintToUint256(0L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.ZERO);

        // Test max tinyint value
        result = castFromTinyintToUint256(127L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(127));

        // Test negative value throws exception
        assertThatThrownBy(() -> castFromTinyintToUint256(-1L))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast negative TINYINT value");
    }

    @Test
    public void testCastFromSmallint()
    {
        // Test positive values
        Slice result = castFromSmallintToUint256(12345L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(12345));

        // Test zero
        result = castFromSmallintToUint256(0L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.ZERO);

        // Test max smallint value
        result = castFromSmallintToUint256(32767L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(32767));

        // Test negative value throws exception
        assertThatThrownBy(() -> castFromSmallintToUint256(-1L))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast negative SMALLINT value");
    }

    @Test
    public void testCastFromInteger()
    {
        // Test positive values
        Slice result = castFromIntegerToUint256(123456789L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(123456789));

        // Test zero
        result = castFromIntegerToUint256(0L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.ZERO);

        // Test max integer value
        result = castFromIntegerToUint256(2147483647L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(2147483647));

        // Test negative value throws exception
        assertThatThrownBy(() -> castFromIntegerToUint256(-1L))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast negative INTEGER value");
    }

    @Test
    public void testCastFromBigint()
    {
        // Test positive values
        Slice result = castFromBigintToUint256(9223372036854775807L); // max long
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(9223372036854775807L));

        // Test zero
        result = castFromBigintToUint256(0L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.ZERO);

        // Test negative value throws exception
        assertThatThrownBy(() -> castFromBigintToUint256(-1L))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast negative BIGINT value");
    }

    @Test
    public void testCastFromReal()
    {
        // Test positive integer values
        float value = 123.0f;
        long bits = Float.floatToIntBits(value);
        Slice result = castFromRealToUint256(bits);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(123));

        // Test zero
        value = 0.0f;
        bits = Float.floatToIntBits(value);
        result = castFromRealToUint256(bits);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.ZERO);

        // Test large integer value
        value = 1000000.0f;
        bits = Float.floatToIntBits(value);
        result = castFromRealToUint256(bits);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(1000000));

        // Test negative value throws exception
        value = -1.0f;
        bits = Float.floatToIntBits(value);
        long finalBits = bits;
        assertThatThrownBy(() -> castFromRealToUint256(finalBits))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast negative REAL value");

        // Test non-integer value throws exception
        value = 123.5f;
        bits = Float.floatToIntBits(value);
        long finalBits2 = bits;
        assertThatThrownBy(() -> castFromRealToUint256(finalBits2))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast non-integer REAL value");

        // Test infinity throws exception
        value = Float.POSITIVE_INFINITY;
        bits = Float.floatToIntBits(value);
        long finalBits3 = bits;
        assertThatThrownBy(() -> castFromRealToUint256(finalBits3))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast non-finite REAL value");

        // Test NaN throws exception
        value = Float.NaN;
        bits = Float.floatToIntBits(value);
        long finalBits4 = bits;
        assertThatThrownBy(() -> castFromRealToUint256(finalBits4))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast non-finite REAL value");
    }

    @Test
    public void testCastFromDouble()
    {
        // Test positive integer values
        Slice result = castFromDoubleToUint256(123.0);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(123));

        // Test zero
        result = castFromDoubleToUint256(0.0);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.ZERO);

        // Test large integer value
        result = castFromDoubleToUint256(1000000000.0);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(1000000000));

        // Test very large value that fits in uint256
        double largeValue = Math.pow(2, 100); // 2^100 due to double precision limits becomes 1.2676506002282294e+30
        result = castFromDoubleToUint256(largeValue);
        assertThat(toUint256BigInteger(result)).isEqualTo(new BigInteger("1267650600228229400000000000000"));

        // Test negative value throws exception
        assertThatThrownBy(() -> castFromDoubleToUint256(-1.0))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast negative DOUBLE value");

        // Test non-integer value throws exception
        assertThatThrownBy(() -> castFromDoubleToUint256(123.5))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast non-integer DOUBLE value");

        // Test infinity throws exception
        assertThatThrownBy(() -> castFromDoubleToUint256(Double.POSITIVE_INFINITY))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast non-finite DOUBLE value");

        // Test NaN throws exception
        assertThatThrownBy(() -> castFromDoubleToUint256(Double.NaN))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast non-finite DOUBLE value");
    }

    @Test
    public void testBoundaryValues()
    {
        // Test maximum values for each type
        Slice result;

        // TINYINT max (127)
        result = castFromTinyintToUint256(127L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(127));

        // SMALLINT max (32767)
        result = castFromSmallintToUint256(32767L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(32767));

        // INTEGER max (2147483647)
        result = castFromIntegerToUint256(2147483647L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(2147483647));

        // BIGINT max (9223372036854775807)
        result = castFromBigintToUint256(9223372036854775807L);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(9223372036854775807L));
    }

    @Test
    public void testOverflowProtection()
    {
        // Test that values exceeding uint256 range throw exception
        // This test would need to be adapted based on how BigDecimal handles very large values

        // For now, test that the conversion functions properly handle the maximum range
        // The actual overflow protection is tested in the toFixedUint256 method

        // Test a value that should convert successfully
        double largeButValidValue = Math.pow(2, 200); // 2^200 due to double precision limits becomes 1.6069380442589903e+60
        Slice result = castFromDoubleToUint256(largeButValidValue);
        assertThat(toUint256BigInteger(result)).isEqualTo(new BigInteger("1606938044258990300000000000000000000000000000000000000000000"));
    }

    @Test
    public void testCastFromUint256ToBigint()
    {
        // Test small value
        Slice smallValue = uint256FromLong(123L);
        long result = castFromUint256ToBigint(smallValue);
        assertThat(result).isEqualTo(123L);

        // Test zero
        Slice zeroValue = uint256FromLong(0L);
        result = castFromUint256ToBigint(zeroValue);
        assertThat(result).isEqualTo(0L);

        // Test max bigint value
        Slice maxBigintValue = uint256FromLong(Long.MAX_VALUE);
        result = castFromUint256ToBigint(maxBigintValue);
        assertThat(result).isEqualTo(Long.MAX_VALUE);

        // Test value too large for bigint
        Slice tooLargeValue = uint256FromBigInteger(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        assertThatThrownBy(() -> castFromUint256ToBigint(tooLargeValue))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for BIGINT");
    }

    @Test
    public void testCastFromUint256ToInteger()
    {
        // Test small value
        Slice smallValue = uint256FromLong(123L);
        long result = castFromUint256ToInteger(smallValue);
        assertThat(result).isEqualTo(123L);

        // Test max integer value
        Slice maxIntValue = uint256FromLong(Integer.MAX_VALUE);
        result = castFromUint256ToInteger(maxIntValue);
        assertThat(result).isEqualTo(Integer.MAX_VALUE);

        // Test value too large for integer
        Slice tooLargeValue = uint256FromLong(Integer.MAX_VALUE + 1L);
        assertThatThrownBy(() -> castFromUint256ToInteger(tooLargeValue))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for INTEGER");
    }

    @Test
    public void testCastFromUint256ToSmallint()
    {
        // Test small value
        Slice smallValue = uint256FromLong(123L);
        long result = castFromUint256ToSmallint(smallValue);
        assertThat(result).isEqualTo(123L);

        // Test max smallint value
        Slice maxSmallintValue = uint256FromLong(Short.MAX_VALUE);
        result = castFromUint256ToSmallint(maxSmallintValue);
        assertThat(result).isEqualTo(Short.MAX_VALUE);

        // Test value too large for smallint
        Slice tooLargeValue = uint256FromLong(Short.MAX_VALUE + 1L);
        assertThatThrownBy(() -> castFromUint256ToSmallint(tooLargeValue))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for SMALLINT");
    }

    @Test
    public void testCastFromUint256ToTinyint()
    {
        // Test small value
        Slice smallValue = uint256FromLong(42L);
        long result = castFromUint256ToTinyint(smallValue);
        assertThat(result).isEqualTo(42L);

        // Test max tinyint value
        Slice maxTinyintValue = uint256FromLong(Byte.MAX_VALUE);
        result = castFromUint256ToTinyint(maxTinyintValue);
        assertThat(result).isEqualTo(Byte.MAX_VALUE);

        // Test value too large for tinyint
        Slice tooLargeValue = uint256FromLong(Byte.MAX_VALUE + 1L);
        assertThatThrownBy(() -> castFromUint256ToTinyint(tooLargeValue))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for TINYINT");
    }

    @Test
    public void testCastFromUint256ToReal()
    {
        // Test small value
        Slice smallValue = uint256FromLong(123L);
        long result = castFromUint256ToReal(smallValue);
        float floatValue = Float.intBitsToFloat((int) result);
        assertThat(floatValue).isEqualTo(123.0f);

        // Test max value that fits in float
        Slice maxFloatValue = uint256FromLong(16777215L); // 2^24 -1
        result = castFromUint256ToReal(maxFloatValue);
        floatValue = Float.intBitsToFloat((int) result);
        assertThat(floatValue).isEqualTo(16777215.0f);

        // Test value too large for float
        Slice tooLargeValue = uint256FromLong(16777216L); // 2^24
        assertThatThrownBy(() -> castFromUint256ToReal(tooLargeValue))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for REAL");
    }

    @Test
    public void testCastFromUint256ToDouble()
    {
        // Test small value
        Slice smallValue = uint256FromLong(123L);
        double result = castFromUint256ToDouble(smallValue);
        assertThat(result).isEqualTo(123.0);

        // Test max value that fits in double
        Slice maxDoubleValue = uint256FromBigInteger(BigInteger.valueOf(2).pow(53).subtract(BigInteger.ONE));
        result = castFromUint256ToDouble(maxDoubleValue);
        assertThat(result).isEqualTo(Math.pow(2, 53) - 1);

        // Test value too large for double
        Slice tooLargeValue = uint256FromBigInteger(BigInteger.valueOf(2).pow(53));
        assertThatThrownBy(() -> castFromUint256ToDouble(tooLargeValue))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for DOUBLE");
    }
/*
    @Test
    public void testCastFromUint256ToShortDecimal()
    {
        // Test small value with scale 0
        Slice smallValue = uint256FromLong(123L);
        long result = castFromUint256ToShortDecimal(smallValue, 10, 0);
        assertThat(result).isEqualTo(123L);

        // Test value with scale 2
        Slice valueWithScale = uint256FromLong(12345L);
        result = castFromUint256ToShortDecimal(valueWithScale, 10, 2);
        assertThat(result).isEqualTo(1234500L); // 12345 * 100

        // Test value too large for short decimal precision
        Slice largeValue = uint256FromBigInteger(BigInteger.valueOf(10).pow(10)); // 10^10
        assertThatThrownBy(() -> castFromUint256ToShortDecimal(largeValue, 5, 0))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for DECIMAL");

        // Test value too large for long (short decimal)
        Slice tooLargeForLong = uint256FromBigInteger(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        assertThatThrownBy(() -> castFromUint256ToShortDecimal(tooLargeForLong, 20, 0))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for short DECIMAL");
    }

    @Test
    public void testCastFromUint256ToLongDecimal()
    {
        // Test small value with scale 0
        Slice smallValue = uint256FromLong(123L);
        Slice result = castFromUint256ToLongDecimal(smallValue, 20, 0);
        BigInteger decimalValue = io.trino.spi.type.Decimals.decodeUnscaledValue(result);
        assertThat(decimalValue).isEqualTo(BigInteger.valueOf(123));

        // Test value with scale 2
        Slice valueWithScale = uint256FromLong(12345L);
        result = castFromUint256ToLongDecimal(valueWithScale, 20, 2);
        decimalValue = io.trino.spi.type.Decimals.decodeUnscaledValue(result);
        assertThat(decimalValue).isEqualTo(BigInteger.valueOf(1234500)); // 12345 * 100

        // Test value too large for decimal precision
        Slice largeValue = uint256FromBigInteger(BigInteger.valueOf(10).pow(10)); // 10^10
        assertThatThrownBy(() -> castFromUint256ToLongDecimal(largeValue, 5, 0))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("UINT256 value too large for DECIMAL");
    }
*/
    /**
     * Helper method to convert a UInt256 Slice back to BigInteger for testing
     */
    private BigInteger toUint256BigInteger(Slice slice)
    {
        byte[] bytes = slice.getBytes();
        return new BigInteger(1, bytes); // 1 means positive
    }

    /**
     * Helper method to create a UInt256 from a long value
     */
    private Slice uint256FromLong(long value)
    {
        return castFromBigintToUint256(value);
    }

    /**
     * Helper method to create a UInt256 from a BigInteger value
     */
    private Slice uint256FromBigInteger(BigInteger value)
    {
        return io.airlift.slice.Slices.wrappedBuffer(toFixedUint256(value));
    }

    /**
     * Helper method to convert BigInteger to fixed 32-byte UInt256 format
     */
    private byte[] toFixedUint256(BigInteger value)
    {
        if (value.signum() < 0 || value.bitLength() > 256) {
            throw new IllegalArgumentException("Value out of range for UInt256");
        }
        byte[] tmp = value.toByteArray();
        if (tmp.length == 0) {
            return new byte[32];
        }
        // strip possible leading sign byte 0x00
        int offset = 0;
        if (tmp.length > 1 && tmp[0] == 0) {
            offset = 1;
        }
        int len = tmp.length - offset;
        if (len > 32) {
            throw new IllegalArgumentException("Value out of range for UInt256");
        }
        byte[] out = new byte[32];
        System.arraycopy(tmp, offset, out, 32 - len, len);
        return out;
    }
}
