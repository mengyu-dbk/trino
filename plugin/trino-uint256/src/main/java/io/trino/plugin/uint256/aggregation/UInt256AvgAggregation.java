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
import io.trino.plugin.uint256.aggregation.state.UInt256CountAndSumState;
import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import static io.trino.plugin.uint256.type.UInt256Type.UINT256;

@AggregationFunction("avg")
public final class UInt256AvgAggregation
{
    private static final UInt256Type type = UINT256;

    private UInt256AvgAggregation() {}

    @InputFunction
    public static void avg(@AggregationState UInt256CountAndSumState state, @SqlType(UInt256Type.NAME) Slice value)
    {
        if (state.getSum() == null) {
            state.setSum(value);
        }
        else {
            state.setSum(UInt256Operators.add(state.getSum(), value));
        }
        state.setCount(state.getCount() + 1);
    }

    @CombineFunction
    public static void combine(@AggregationState UInt256CountAndSumState state, @AggregationState UInt256CountAndSumState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }
        if (state.getSum() == null) {
            state.setSum(otherState.getSum());
        }
        else {
            state.setSum(UInt256Operators.add(state.getSum(), otherState.getSum()));
        }
        state.setCount(state.getCount() + otherState.getCount());
    }

    @OutputFunction(UInt256Type.NAME)
    public static void output(@AggregationState UInt256CountAndSumState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            // 计算平均值：sum / count
            Slice countAsUInt256 = UInt256Operators.castFromBigintToUint256(state.getCount());
            Slice avgResult = UInt256Operators.divide(state.getSum(), countAsUInt256);
            type.writeSlice(out, avgResult);
        }
    }
}
