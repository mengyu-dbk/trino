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
package io.trino.plugin.uint256.type;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

public class UInt256Type
        extends AbstractVariableWidthType
{
    public static final UInt256Type UINT256 = new UInt256Type();
    public static final String NAME = "UINT256";

    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = TypeOperatorDeclaration.builder(Slice.class)
            .addOperators(DEFAULT_READ_OPERATORS)
            .addOperators(DEFAULT_COMPARABLE_OPERATORS)
            .addOperators(DEFAULT_ORDERING_OPERATORS)
            .build();
    public static final int UINT256_BYTE_LENGTH = 32;

    public UInt256Type()
    {
        super(new TypeSignature(NAME), Slice.class);
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return getSlice(block, position).getBytes();
    }

    @Override
    public VariableWidthBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return super.createBlockBuilder(blockBuilderStatus, expectedEntries, UINT256_BYTE_LENGTH);
    }

    @Override
    public VariableWidthBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        if (expectedBytesPerEntry != UINT256_BYTE_LENGTH) {
            throw new IllegalArgumentException("UINT256 block entry length should be 32 bytes");
        }
        return super.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        Slice res = valueBlock.getSlice(valuePosition);
        if (res.length() != UINT256_BYTE_LENGTH) {
            throw new IllegalArgumentException("UINT256 length should be 32 bytes");
        }
        return res;
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        if (value.length() != UINT256_BYTE_LENGTH) {
            throw new IllegalArgumentException("UINT256 length should be 32 bytes");
        }
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        if (length != UINT256_BYTE_LENGTH) {
            throw new IllegalArgumentException("UINT256 length should be 32 bytes");
        }
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value, offset, length);
    }

    @Override
    public boolean equals(Object other)
    {
        return other == UINT256;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
