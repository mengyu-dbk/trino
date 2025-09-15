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

import java.math.BigInteger;

import static io.trino.plugin.uint256.type.UInt256Type.UINT256;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;

@AggregationFunction("avg")
public final class UInt256AvgAggregation
{
    private static final UInt256Type type = UINT256;

    private UInt256AvgAggregation() {}

    @InputFunction
    public static void avg(@AggregationState UInt256AvgState state, @SqlType(UInt256Type.NAME) Slice value)
    {
        if (state.getSum() == null) {
            // 第一个值，直接存储为当前平均值
            state.setSum(value);
        }
        else {
            // 使用增量更新公式：new_avg = old_avg + (new_value - old_avg) / (count + 1)
            BigInteger oldAvg = UInt256Operators.getBigInteger(state.getSum());
            BigInteger newValue = UInt256Operators.getBigInteger(value);
            long newCount = state.getCount() + 1;

            // 计算 (newValue - oldAvg) / newCount
            BigInteger diff = newValue.subtract(oldAvg);
            BigInteger increment = diff.divide(BigInteger.valueOf(newCount));

            // 计算新的平均值
            BigInteger newAvg = oldAvg.add(increment);

            // 将新平均值转换回UInt256格式存储
            byte[] newAvgBytes = toFixedUint256(newAvg);
            state.setSum(io.airlift.slice.Slices.wrappedBuffer(newAvgBytes));
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
            // 合并两个平均值：combined_avg = (avg1 * count1 + avg2 * count2) / (count1 + count2)
            BigInteger avg1 = UInt256Operators.getBigInteger(state.getSum());
            BigInteger avg2 = UInt256Operators.getBigInteger(otherState.getSum());
            long count1 = state.getCount();
            long count2 = otherState.getCount();

            // 计算加权和
            BigInteger sum1 = avg1.multiply(BigInteger.valueOf(count1));
            BigInteger sum2 = avg2.multiply(BigInteger.valueOf(count2));
            BigInteger totalSum = sum1.add(sum2);

            // 计算新的平均值
            BigInteger totalCount = BigInteger.valueOf(count1 + count2);
            BigInteger combinedAvg = totalSum.divide(totalCount);

            // 将合并后的平均值转换回UInt256格式存储
            byte[] combinedAvgBytes = toFixedUint256(combinedAvg);
            state.setSum(io.airlift.slice.Slices.wrappedBuffer(combinedAvgBytes));
            state.setCount(count1 + count2);
        }
    }

    @OutputFunction(UInt256Type.NAME)
    public static void output(@AggregationState UInt256AvgState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            // 直接返回存储的平均值
            type.writeSlice(out, state.getSum());
        }
    }

    private static byte[] toFixedUint256(BigInteger value)
    {
        if (value.signum() < 0) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "uint256 value cannot be negative");
        }
        if (value.bitLength() > 256) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "uint256 value out of range");
        }
        byte[] tmp = value.toByteArray(); // big-endian, may contain leading zero
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
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "uint256 value out of range");
        }
        byte[] out = new byte[32];
        System.arraycopy(tmp, offset, out, 32 - len, len);
        return out;
    }
}
