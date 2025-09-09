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
package io.trino.plugin.uint256.aggregation.state;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.plugin.uint256.type.UInt256Type.UINT256;
import static io.trino.spi.type.BigintType.BIGINT;

public class UInt256CountAndSumStateSerializer
        implements AccumulatorStateSerializer<UInt256CountAndSumState>
{
    private static final RowType TYPE = RowType.from(java.util.List.of(
            new RowType.Field(java.util.Optional.of("count"), BIGINT),
            new RowType.Field(java.util.Optional.of("sum"), UINT256)));

    @Override
    public Type getSerializedType()
    {
        return TYPE;
    }

    @Override
    public void serialize(UInt256CountAndSumState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
            return;
        }

        RowBlockBuilder rowBuilder = (RowBlockBuilder) TYPE.createBlockBuilder(null, 1);
        rowBuilder.buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), state.getCount());
            UINT256.writeSlice(fieldBuilders.get(1), state.getSum());
        });
        out.append(rowBuilder.buildValueBlock(), 0);
    }

    @Override
    public void deserialize(Block block, int index, UInt256CountAndSumState state)
    {
        if (block.isNull(index)) {
            state.setCount(0);
            state.setSum(null);
            return;
        }

        io.trino.spi.block.SqlRow sqlRow = (io.trino.spi.block.SqlRow) TYPE.getObject(block, index);
        state.setCount(BIGINT.getLong(sqlRow.getUnderlyingFieldBlock(0), sqlRow.getUnderlyingFieldPosition(0)));
        state.setSum(UINT256.getSlice(sqlRow.getUnderlyingFieldBlock(1), sqlRow.getUnderlyingFieldPosition(1)));
    }
}
