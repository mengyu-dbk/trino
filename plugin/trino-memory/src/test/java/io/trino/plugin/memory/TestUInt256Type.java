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
package io.trino.plugin.memory;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.memory.type.UInt256Type;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUInt256Type
{
    private final UInt256Type uint256Type = UInt256Type.UINT256;

    @Test
    public void testBasicProperties()
    {
        assertThat(uint256Type.getDisplayName()).isEqualTo("uint256");
        assertThat(uint256Type.isComparable()).isTrue();
        assertThat(uint256Type.isOrderable()).isTrue();
        assertThat(uint256Type.getJavaType()).isEqualTo(Slice.class);
    }

    @Test
    public void testBlockBuilderAndReader()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 1);

        // 创建一个32字节的测试数据
        byte[] testData = new byte[32];
        for (int i = 0; i < 32; i++) {
            testData[i] = (byte) i;
        }
        Slice testSlice = Slices.wrappedBuffer(testData);

        // 写入数据
        uint256Type.writeSlice(blockBuilder, testSlice);

        // 构建block
        Block block = blockBuilder.build();

        // 验证数据
        assertThat(block.isNull(0)).isFalse();
        Slice readSlice = uint256Type.getSlice(block, 0);
        assertThat(readSlice.length()).isEqualTo(32);
        assertThat(readSlice.getBytes()).isEqualTo(testData);

        // 验证Object值
        Object objectValue = uint256Type.getObjectValue(block, 0);
        assertThat(objectValue).isNotNull();
        assertThat(objectValue).isEqualTo(testData);
    }

    @Test
    public void testNullHandling()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 1);
        blockBuilder.appendNull();

        Block block = blockBuilder.build();
        assertThat(block.isNull(0)).isTrue();

        Object objectValue = uint256Type.getObjectValue(block, 0);
        assertThat(objectValue).isNull();
    }

    @Test
    public void testMultipleValues()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 3);

        // 写入多个值
        for (int i = 0; i < 3; i++) {
            byte[] testData = new byte[32];
            testData[0] = (byte) i;
            Slice testSlice = Slices.wrappedBuffer(testData);
            uint256Type.writeSlice(blockBuilder, testSlice);
        }

        Block block = blockBuilder.build();
        assertThat(block.getPositionCount()).isEqualTo(3);

        // 验证每个值
        for (int i = 0; i < 3; i++) {
            assertThat(block.isNull(i)).isFalse();
            Slice readSlice = uint256Type.getSlice(block, i);
            assertThat(readSlice.getByte(0)).isEqualTo(i);
        }
    }

    @Test
    public void testTypeSignature()
    {
        assertThat(uint256Type.getTypeSignature().getBase()).isEqualTo("uint256");
        assertThat(uint256Type.getTypeSignature().getParameters()).isEmpty();
    }
}
