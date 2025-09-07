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

@AggregationFunction("sum")
public final class UInt256SumAggregation
{
    private static final UInt256Type type = UINT256.UINT256;

    private UInt256SumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState UInt256CountAndSumState state, @SqlType(UInt256Type.NAME) Slice value)
    {
        state.setSum(UInt256Operators.getLong(value));
        state.setCount(0);
    }

    @CombineFunction
    public static void combine(@AggregationState UInt256CountAndSumState state, @AggregationState UInt256CountAndSumState otherState)
    {
        state.setSum(state.getSum() + otherState.getSum());
        state.setCount(state.getCount() + otherState.getCount());
    }

    @OutputFunction(UInt256Type.NAME)
    public static void output(@AggregationState UInt256CountAndSumState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            type.writeSlice(out, UInt256Operators.castFromBigintToUint256(state.getSum()));
        }
    }
}
