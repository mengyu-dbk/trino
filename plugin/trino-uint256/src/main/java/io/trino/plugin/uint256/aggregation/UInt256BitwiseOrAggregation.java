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
import io.trino.plugin.uint256.aggregation.state.UInt256BitwiseState;
import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import static io.trino.plugin.uint256.type.UInt256Type.UINT256;

@AggregationFunction("bitwise_or_agg")
public final class UInt256BitwiseOrAggregation
{
    private static final UInt256Type type = UINT256;

    private UInt256BitwiseOrAggregation() {}

    @InputFunction
    public static void bitwiseOrAgg(@AggregationState UInt256BitwiseState state, @SqlType(UInt256Type.NAME) Slice value)
    {
        if (value == null) {
            return; // Skip null values
        }
        if (state.getValue() == null) {
            state.setValue(value);
        }
        else {
            state.setValue(UInt256Operators.bitwiseOr(state.getValue(), value));
        }
    }

    @CombineFunction
    public static void combine(@AggregationState UInt256BitwiseState state, @AggregationState UInt256BitwiseState otherState)
    {
        Slice otherValue = otherState.getValue();
        if (otherValue == null) {
            return;
        }
        if (state.getValue() == null) {
            state.setValue(otherValue);
        }
        else {
            state.setValue(UInt256Operators.bitwiseOr(state.getValue(), otherValue));
        }
    }

    @OutputFunction(UInt256Type.NAME)
    public static void output(@AggregationState UInt256BitwiseState state, BlockBuilder out)
    {
        if (state.getValue() == null) {
            out.appendNull();
        }
        else {
            type.writeSlice(out, state.getValue());
        }
    }
}
