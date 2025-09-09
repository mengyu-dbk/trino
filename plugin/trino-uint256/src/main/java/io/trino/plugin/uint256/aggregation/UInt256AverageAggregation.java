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
public final class UInt256AverageAggregation
{
    private UInt256AverageAggregation() {}

    @InputFunction
    public static void input(@AggregationState UInt256CountAndSumState state, @SqlType(UInt256Type.NAME) Slice value)
    {
        state.setCount(state.getCount() + 1);
        if (state.getSum() == null) {
            state.setSum(value);
        }
        else {
            state.setSum(UInt256Operators.add(state.getSum(), value));
        }
    }

    @CombineFunction
    public static void combine(@AggregationState UInt256CountAndSumState state, @AggregationState UInt256CountAndSumState otherState)
    {
        state.setCount(state.getCount() + otherState.getCount());
        if (state.getSum() == null) {
            state.setSum(otherState.getSum());
        }
        else if (otherState.getSum() != null) {
            state.setSum(UInt256Operators.add(state.getSum(), otherState.getSum()));
        }
    }

    @OutputFunction(UInt256Type.NAME)
    public static void output(@AggregationState UInt256CountAndSumState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count == 0) {
            out.appendNull();
            return;
        }
        Slice sum = state.getSum();
        if (sum == null) {
            out.appendNull();
            return;
        }
        Slice countAsUint256 = UInt256Operators.castFromBigintToUint256(count);
        Slice average = UInt256Operators.divide(sum, countAsUint256);
        UINT256.writeSlice(out, average);
    }
}
