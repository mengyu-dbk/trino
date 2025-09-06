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
package io.trino.plugin.uint256;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestUInt256Query
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
        for (byte i = 0; i < 3; i++) {
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

    @Test
    public void testBoundaryValues()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 2);

        // 测试最小值 (0)
        byte[] zeroBytes = new byte[32];
        Slice zeroSlice = Slices.wrappedBuffer(zeroBytes);
        uint256Type.writeSlice(blockBuilder, zeroSlice);

        // 测试最大值 (2^256 - 1)
        byte[] maxBytes = new byte[32];
        for (int i = 0; i < 32; i++) {
            maxBytes[i] = (byte) 0xFF;
        }
        Slice maxSlice = Slices.wrappedBuffer(maxBytes);
        uint256Type.writeSlice(blockBuilder, maxSlice);

        Block block = blockBuilder.build();

        // 验证最小值
        Slice readZero = uint256Type.getSlice(block, 0);
        assertThat(readZero.getBytes()).isEqualTo(zeroBytes);

        // 验证最大值
        Slice readMax = uint256Type.getSlice(block, 1);
        assertThat(readMax.getBytes()).isEqualTo(maxBytes);
    }

    @Test
    public void testInvalidLength()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 1);

        // 测试长度不足32字节的数据
        byte[] shortData = new byte[16];
        Slice shortSlice = Slices.wrappedBuffer(shortData);

        // 这应该正常工作，因为AbstractVariableWidthType支持可变长度
        uint256Type.writeSlice(blockBuilder, shortSlice);

        Block block = blockBuilder.build();
        Slice readSlice = uint256Type.getSlice(block, 0);
        assertThat(readSlice.length()).isEqualTo(16);
    }

    @Test
    public void testOverflowLength()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 1);

        // 测试超过32字节的数据
        byte[] longData = new byte[64];
        Slice longSlice = Slices.wrappedBuffer(longData);

        // 这应该正常工作，因为AbstractVariableWidthType支持可变长度
        uint256Type.writeSlice(blockBuilder, longSlice);

        Block block = blockBuilder.build();
        Slice readSlice = uint256Type.getSlice(block, 0);
        assertThat(readSlice.length()).isEqualTo(64);
    }

    @Test
    public void testBlockBuilderCapacity()
    {
        // 测试BlockBuilder的容量管理
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 10, 32);

        // 写入多个值
        for (int i = 0; i < 10; i++) {
            byte[] testData = new byte[32];
            testData[0] = (byte) i;
            Slice testSlice = Slices.wrappedBuffer(testData);
            uint256Type.writeSlice(blockBuilder, testSlice);
        }

        Block block = blockBuilder.build();
        assertThat(block.getPositionCount()).isEqualTo(10);

        // 验证所有值
        for (int i = 0; i < 10; i++) {
            Slice readSlice = uint256Type.getSlice(block, i);
            assertThat(readSlice.getByte(0)).isEqualTo((byte) i);
        }
    }

    @Test
    public void testAppendTo()
    {
        // 测试appendTo方法
        BlockBuilder sourceBuilder = uint256Type.createBlockBuilder(null, 1);
        byte[] testData = new byte[32];
        testData[0] = 42;
        Slice testSlice = Slices.wrappedBuffer(testData);
        uint256Type.writeSlice(sourceBuilder, testSlice);
        Block sourceBlock = sourceBuilder.build();

        BlockBuilder targetBuilder = uint256Type.createBlockBuilder(null, 1);
        uint256Type.appendTo(sourceBlock, 0, targetBuilder);

        Block targetBlock = targetBuilder.build();
        Slice readSlice = uint256Type.getSlice(targetBlock, 0);
        assertThat(readSlice.getBytes()).isEqualTo(testData);
    }

    @Test
    public void testNullHandlingInMultipleValues()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 3);

        // 写入值、null、值
        byte[] testData1 = new byte[32];
        testData1[0] = 1;
        uint256Type.writeSlice(blockBuilder, Slices.wrappedBuffer(testData1));

        blockBuilder.appendNull();

        byte[] testData2 = new byte[32];
        testData2[0] = 2;
        uint256Type.writeSlice(blockBuilder, Slices.wrappedBuffer(testData2));

        Block block = blockBuilder.build();

        assertThat(block.isNull(0)).isFalse();
        assertThat(block.isNull(1)).isTrue();
        assertThat(block.isNull(2)).isFalse();

        Slice readSlice1 = uint256Type.getSlice(block, 0);
        assertThat(readSlice1.getByte(0)).isEqualTo((byte) 1);

        Slice readSlice2 = uint256Type.getSlice(block, 2);
        assertThat(readSlice2.getByte(0)).isEqualTo((byte) 2);
    }

    @Test
    public void testBigintToUint256()
    {
        // 测试正数
        long value = 123456789L;
        Slice result = UInt256Operators.uint256(value);
        byte[] bytes = result.getBytes();
        assertThat(bytes.length).isEqualTo(32);
        // 高24字节为0
        for (int i = 0; i < 24; i++) {
            assertThat(bytes[i]).isEqualTo((byte) 0);
        }
        // 低8字节为bigint内容
        long reconstructed = 0;
        for (int i = 24; i < 32; i++) {
            reconstructed = (reconstructed << 8) | (bytes[i] & 0xFF);
        }
        assertThat(reconstructed).isEqualTo(value);

        // 测试0
        result = UInt256Operators.uint256(0L);
        bytes = result.getBytes();
        for (byte b : bytes) {
            assertThat(b).isEqualTo((byte) 0);
        }

        // 测试负数（uint256不支持负数，结果为抛出错误）
        assertThatThrownBy(() -> UInt256Operators.uint256(-1L))
                .isInstanceOf(io.trino.spi.TrinoException.class)
                .hasMessageContaining("Cannot cast negative BIGINT value -1 to UINT256");
    }

    @Test
    public void testCastBigintToUint256()
    {
        // 直接调用CAST
        long value = 987654321L;
        Slice result = UInt256Operators.castFromBigintToUint256(value);
        byte[] bytes = result.getBytes();
        assertThat(bytes.length).isEqualTo(32);
        long reconstructed = 0;
        for (int i = 24; i < 32; i++) {
            reconstructed = (reconstructed << 8) | (bytes[i] & 0xFF);
        }
        assertThat(reconstructed).isEqualTo(value);
    }
}
