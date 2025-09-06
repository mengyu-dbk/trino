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
import io.trino.spi.TrinoException;
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
    /*
    @Test
    public void testVarcharCastRoundtrip()
    {
        // '' -> 0
        Slice zero = UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice(""));
        assertThat(zero.length()).isEqualTo(32);
        for (byte b : zero.getBytes()) {
            assertThat(b).isEqualTo((byte) 0);
        }
        // '0x0f' -> 0x0f
        Slice v = UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice("0x0f"));
        String hex = UInt256Operators.castFromUint256ToVarchar(v).toStringUtf8();
        assertThat(hex).endsWith("0f");
        assertThat(hex.length()).isEqualTo(64);

        // 长度校验
        assertThatThrownBy(() -> UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice("F".repeat(65))))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid UINT256 hex length");
        // 非法字符
        assertThatThrownBy(() -> UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice("0xz1")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid hex digit");
    }
    */
    @Test
    public void testAddition()
    {
        // Test: 0 + 0 = 0
        Slice zero = uint256FromHex("0");
        Slice result = UInt256Operators.add(zero, zero);
        assertThat("0000000000000000000000000000000000000000000000000000000000000000").isEqualTo(toHex(result));

        // Test: 1 + 1 = 2
        Slice one = uint256FromHex("1");
        Slice two = uint256FromHex("2");
        result = UInt256Operators.add(one, one);
        assertThat(toHex(two)).isEqualTo(toHex(result));

        // Test: 255 + 1 = 256 (carry propagation)
        Slice ff = uint256FromHex("ff");
        Slice hundred = uint256FromHex("100");
        result = UInt256Operators.add(ff, one);
        assertThat(toHex(hundred)).isEqualTo(toHex(result));

        // Test: large numbers
        Slice large1 = uint256FromHex("123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        Slice large2 = uint256FromHex("fedcba9876543210fedcba9876543210fedcba9876543210fedcba987654321");
        result = UInt256Operators.add(large1, large2);
        // Expected: 1111111111111110111111111111111011111111111111101111111111111110
        assertThat(toHex(result)).isEqualTo("1111111111111110111111111111111011111111111111101111111111111110");
    }

    /*
    @Test
    public void testAdditionOverflow()
    {
        // Test overflow: MAX_UINT256 + 1
        Slice maxUint256 = uint256FromHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        Slice one = uint256FromHex("1");

        TrinoException exception = assertThrows(TrinoException.class,
                () -> UInt256Operators.add(maxUint256, one));
        assertEquals(NUMERIC_VALUE_OUT_OF_RANGE, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("uint256 addition overflow"));
    }

    @Test
    public void testAdditionCarryPropagation()
    {
        // Test multiple byte carry propagation
        Slice value1 = uint256FromHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00");
        Slice value2 = uint256FromHex("ff");
        Slice expected = uint256FromHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        Slice result = UInt256Operators.add(value1, value2);
        assertEquals(toHex(expected), toHex(result));
    }
    */

    private Slice uint256FromHex(String hex)
    {
        // Pad to 64 characters (32 bytes)
        String padded = String.format("%64s", hex).replace(' ', '0');
        byte[] bytes = new byte[32];
        for (int i = 0; i < 32; i++) {
            int index = i * 2;
            int value = Integer.parseInt(padded.substring(index, index + 2), 16);
            bytes[i] = (byte) value;
        }
        return Slices.wrappedBuffer(bytes);
    }

    private String toHex(Slice slice)
    {
        return UInt256Operators.castFromUint256ToVarchar(slice).toStringUtf8();
    }

    @Test
    public void testArithmeticOps()
    {
        // a = 0x0100
        byte[] aBytes = new byte[32];
        aBytes[30] = 0x01;
        aBytes[31] = 0x00;
        Slice a = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(aBytes));
        //b = 0x01
        byte[] bBytes = new byte[32];
        bBytes[30] = 0x00;
        bBytes[31] = 0x01;
        Slice b = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(bBytes));

        // subtract: 0x0100 - 0x01 = 0x00ff
        Slice sub = UInt256Operators.subtract(a, b);
        String subHex = UInt256Operators.castFromUint256ToVarchar(sub).toStringUtf8();
        assertThat(subHex).endsWith("00ff");

        // multiply: 0x02 * 0x03 = 0x06
        Slice m2 = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x02}));
        Slice m3 = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x03}));
        Slice mul = UInt256Operators.multiply(m2, m3);
        assertThat(UInt256Operators.castFromUint256ToVarchar(mul).toStringUtf8()).endsWith("0006");

        // multiply overflow: max * 2
        byte[] max = new byte[32];
        for (int i = 0; i < 32; i++) {
            max[i] = (byte) 0xFF;
        }
        Slice vmax = Slices.wrappedBuffer(max);
        assertThatThrownBy(() -> UInt256Operators.multiply(vmax, m2))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("multiplication overflow");

        // divide: 0x10 / 0x04 = 0x04
        Slice d10 = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x10}));
        Slice d4 = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x04}));
        Slice div = UInt256Operators.divide(d10, d4);
        assertThat(UInt256Operators.castFromUint256ToVarchar(div).toStringUtf8()).endsWith("0004");

        // divide by zero
        Slice zeroU = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x00}));
        assertThatThrownBy(() -> UInt256Operators.divide(d10, zeroU))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Division by zero");
    }

    @Test
    public void testBitwiseOps()
    {
        Slice fzero = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer((byte) 0xF0));
        Slice zerof = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x0F}));

        // and => 0x00
        Slice and = UInt256Operators.bitwiseAnd(fzero, zerof);
        assertThat(UInt256Operators.castFromUint256ToVarchar(and).toStringUtf8()).endsWith("0000");

        // or => 0xFF
        Slice or = UInt256Operators.bitwiseOr(fzero, zerof);
        assertThat(UInt256Operators.castFromUint256ToVarchar(or).toStringUtf8()).endsWith("00ff");

        // xor => 0xFF
        Slice xor = UInt256Operators.bitwiseXor(fzero, zerof);
        assertThat(UInt256Operators.castFromUint256ToVarchar(xor).toStringUtf8()).endsWith("00ff");

        // not(~0x00ff)
        Slice v00ff = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x00, (byte) 0xFF}));
        Slice not = UInt256Operators.bitwiseNot(v00ff);
        String notHex = UInt256Operators.castFromUint256ToVarchar(not).toStringUtf8();

        assertThat(notHex).isEqualTo("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00");
    }
}
