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
package io.trino.plugin.uint256.aggregation;

import io.airlift.slice.Slice;
import io.trino.plugin.uint256.UInt256Operators;
import io.trino.plugin.uint256.aggregation.state.UInt256AvgState;
import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.math.BigInteger;

import static io.trino.plugin.uint256.type.UInt256Type.UINT256;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DoubleType.DOUBLE;

@AggregationFunction("avg")
public final class UInt256AvgAggregation
{
    private static final UInt256Type type = UINT256;

    private UInt256AvgAggregation() {}

    @InputFunction
    public static void avg(@AggregationState UInt256AvgState state, @SqlType(UInt256Type.NAME) Slice value)
    {
        if (state.getSum() == null) {
            state.setSum(value);
        }
        else {
            BigInteger currentSum = extendedBytesToBigInteger(state.getSum());
            BigInteger newValue = UInt256Operators.getBigInteger(value);
            BigInteger newSum = currentSum.add(newValue);

            byte[] newSumBytes = bigIntegerToExtendedBytes(newSum);
            state.setSum(io.airlift.slice.Slices.wrappedBuffer(newSumBytes));
        }
        state.setCount(state.getCount() + 1);
    }

    @CombineFunction
    public static void combine(@AggregationState UInt256AvgState state, @AggregationState UInt256AvgState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }
        if (state.getSum() == null) {
            state.setSum(otherState.getSum());
            state.setCount(otherState.getCount());
        }
        else {
            BigInteger sum1 = extendedBytesToBigInteger(state.getSum());
            BigInteger sum2 = extendedBytesToBigInteger(otherState.getSum());
            BigInteger totalSum = sum1.add(sum2);

            byte[] totalSumBytes = bigIntegerToExtendedBytes(totalSum);
            state.setSum(io.airlift.slice.Slices.wrappedBuffer(totalSumBytes));
            state.setCount(state.getCount() + otherState.getCount());
        }
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState UInt256AvgState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            BigInteger sum = extendedBytesToBigInteger(state.getSum());
            long count = state.getCount();

            java.math.BigDecimal sumDecimal = new java.math.BigDecimal(sum);
            java.math.BigDecimal countDecimal = new java.math.BigDecimal(count);
            java.math.BigDecimal avg = sumDecimal.divide(countDecimal, 34, java.math.RoundingMode.HALF_UP);

            double result = avg.doubleValue();
            DOUBLE.writeDouble(out, result);
        }
    }

    private static byte[] bigIntegerToExtendedBytes(BigInteger value)
    {
        if (value.signum() < 0) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "sum value cannot be negative");
        }
        if (value.equals(BigInteger.ZERO)) {
            return new byte[32];
        }

        byte[] tmp = value.toByteArray();
        if (tmp.length == 0) {
            return new byte[32];
        }

        int offset = 0;
        if (tmp.length > 1 && tmp[0] == 0) {
            offset = 1;
        }
        int actualLen = tmp.length - offset;

        int requiredLen = Math.max(32, actualLen);
        byte[] out = new byte[requiredLen];
        System.arraycopy(tmp, offset, out, requiredLen - actualLen, actualLen);
        return out;
    }

    private static BigInteger extendedBytesToBigInteger(Slice slice)
    {
        if (slice.length() == 32) {
            return UInt256Operators.getBigInteger(slice);
        }

        byte[] bytes = slice.getBytes();
        return new BigInteger(1, bytes);
    }
}
