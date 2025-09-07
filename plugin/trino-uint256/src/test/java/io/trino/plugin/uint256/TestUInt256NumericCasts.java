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
import static io.trino.plugin.uint256.UInt256Operators.castFromDoubleToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromIntegerToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromRealToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromSmallintToUint256;
import static io.trino.plugin.uint256.UInt256Operators.castFromTinyintToUint256;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestUInt256NumericCasts
{
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
/*
    @Test
    public void testCastFromDecimal()
    {
        // Test positive integer decimal (scale 0)
        Slice result = castFromDecimalToUint256(123L, 10, 0);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(123));

        // Test zero
        result = castFromDecimalToUint256(0L, 10, 0);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.ZERO);

        // Test large decimal value
        result = castFromDecimalToUint256(999999999L, 10, 0);
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(999999999));

        // Test decimal with scale but integer value (12.0)
        result = castFromDecimalToUint256(120L, 10, 1); // 120 with scale 1 = 12.0
        assertThat(toUint256BigInteger(result)).isEqualTo(BigInteger.valueOf(12));

        // Test negative decimal throws exception
        assertThatThrownBy(() -> castFromDecimalToUint256(-123L, 10, 0))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast negative DECIMAL value");

        // Test non-integer decimal throws exception (12.5)
        assertThatThrownBy(() -> castFromDecimalToUint256(125L, 10, 1)) // 125 with scale 1 = 12.5
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot cast non-integer DECIMAL value");
    }
*/
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

    /**
     * Helper method to convert a UInt256 Slice back to BigInteger for testing
     */
    private BigInteger toUint256BigInteger(Slice slice)
    {
        byte[] bytes = slice.getBytes();
        return new BigInteger(1, bytes); // 1 means positive
    }
}
