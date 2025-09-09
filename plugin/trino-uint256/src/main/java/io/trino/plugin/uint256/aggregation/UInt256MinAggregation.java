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
import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import static io.trino.plugin.uint256.type.UInt256Type.UINT256;

@AggregationFunction("min")
public final class UInt256MinAggregation
{
    private UInt256MinAggregation() {}

    @InputFunction
    public static void input(State state, @SqlType(UInt256Type.NAME) Slice value)
    {
        if (state.value == null) {
            state.value = value;
        }
        else {
            if (UInt256Operators.getBigInteger(value).compareTo(UInt256Operators.getBigInteger(state.value)) < 0) {
                state.value = value;
            }
        }
    }

    @CombineFunction
    public static void combine(State state, State other)
    {
        if (state.value == null) {
            state.value = other.value;
        }
        else if (other.value != null && UInt256Operators.getBigInteger(other.value).compareTo(UInt256Operators.getBigInteger(state.value)) < 0) {
            state.value = other.value;
        }
    }

    @OutputFunction(UInt256Type.NAME)
    public static void output(State state, BlockBuilder out)
    {
        if (state.value == null) {
            out.appendNull();
        }
        else {
            UINT256.writeSlice(out, state.value);
        }
    }

    public static class State
            implements io.trino.spi.function.AccumulatorState
    {
        private Slice value;

        @Override
        public long getEstimatedSize()
        {
            return value == null ? 0 : value.getRetainedSize();
        }
    }
}
