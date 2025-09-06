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

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestUInt256Query
{
    private final UInt256Type uint256Type = UInt256Type.UINT256;

    @Test
    public void testBasicProperties()
    {
        // 测试类型名称和显示名称
        assertThat(uint256Type.getDisplayName()).isEqualTo(UInt256Type.NAME);
        assertThat(uint256Type.getTypeSignature().getBase()).isEqualTo(UInt256Type.NAME);

        // 测试比较和排序能力
        assertThat(uint256Type.isComparable()).isTrue();
        assertThat(uint256Type.isOrderable()).isTrue();

        // 测试Java类型映射
        System.out.println(uint256Type.getJavaType());
        assertThat(uint256Type.getJavaType()).isEqualTo(Slice.class);

        // 测试类型签名参数
        System.out.println(uint256Type.getTypeSignature());
        assertThat(uint256Type.getTypeSignature().getParameters()).isEmpty();

        // 测试类型类别（如果有的话）
        System.out.println(uint256Type.getTypeId());
        assertThat(uint256Type.getTypeId()).isNotNull();
    }

    @Test
    public void testBlockBuilderAndReader()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 2); // 明确指定容量为2

        // 创建一个32字节的测试数据
        byte[] testData = new byte[32];
        for (int i = 0; i < 32; i++) {
            testData[i] = (byte) i;
        }
        Slice testSlice = Slices.wrappedBuffer(testData);

        byte[] testData2 = testData.clone();
        testData2[0] = 99;
        Slice testSliceCopy = Slices.wrappedBuffer(testData2);

        // 写入数据
        uint256Type.writeSlice(blockBuilder, testSlice);
        uint256Type.writeSlice(blockBuilder, testSliceCopy); // 测试写入副本

        // 构建block
        Block block = blockBuilder.build();

        // 验证block基本属性
        assertThat(block.getPositionCount()).isEqualTo(2);

        // 验证数据1
        assertThat(block.isNull(0)).isFalse();
        Slice readSlice = uint256Type.getSlice(block, 0);
        assertThat(readSlice.length()).isEqualTo(32);
        assertThat(readSlice.getBytes()).isEqualTo(testData);

        // 验证数据2
        assertThat(block.isNull(1)).isFalse();
        Slice readSlice2 = uint256Type.getSlice(block, 1);
        assertThat(readSlice2.length()).isEqualTo(32);
        assertThat(readSlice2.getBytes()).isEqualTo(testData2);

        // 验证数据不相等
        assertThat(readSlice.getBytes()).isNotEqualTo(testData2);
        assertThat(readSlice2.getBytes()).isNotEqualTo(testData);

        // 测试越界访问
        assertThatThrownBy(() -> block.isNull(2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> uint256Type.getSlice(block, 2))
                .isInstanceOf(IllegalArgumentException.class);

        // 验证Object值
        Object objectValue = uint256Type.getObjectValue(block, 0);
        assertThat(objectValue).isNotNull();
        assertThat(objectValue).isEqualTo(testData);

        Object objectValue2 = uint256Type.getObjectValue(block, 1);
        assertThat(objectValue2).isNotNull();
        assertThat(objectValue2).isEqualTo(testData2);

        // 验证数据独立性（修改原始数据不影响存储的数据）
        testData[0] = 127;
        Slice readSliceAgain = uint256Type.getSlice(block, 0);
        assertThat(readSliceAgain.getByte(0)).isEqualTo((byte) 0); // 应该仍是原来的值
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
        assertThat(uint256Type.getTypeSignature().getBase()).isEqualTo(UInt256Type.NAME);
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
    public void testInvalidLengthTooShort()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 1);

        // 测试长度不足32字节的数据
        byte[] shortData = new byte[16];
        Slice shortSlice = Slices.wrappedBuffer(shortData);

        assertThatThrownBy(() -> uint256Type.writeSlice(blockBuilder, shortSlice))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UINT256 length should be 32 bytes");
    }

    @Test
    public void testInvalidLengthTooLong()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 1);

        // 测试超过32字节的数据
        byte[] longData = new byte[64];
        longData[0] = 1;
        longData[63] = 2;
        Slice longSlice = Slices.wrappedBuffer(longData);

        assertThatThrownBy(() -> uint256Type.writeSlice(blockBuilder, longSlice))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UINT256 length should be 32 bytes");
    }

    @Test
    public void testInvalidLengthEdgeCases()
    {
        BlockBuilder blockBuilder = uint256Type.createBlockBuilder(null, 3);

        // 测试空数据
        byte[] emptyData = new byte[0];
        Slice emptySlice = Slices.wrappedBuffer(emptyData);
        assertThatThrownBy(() -> uint256Type.writeSlice(blockBuilder, emptySlice))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UINT256 length should be 32 bytes");

        // 测试31字节（差1字节）
        byte[] almostCorrectData = new byte[31];
        Slice almostCorrectSlice = Slices.wrappedBuffer(almostCorrectData);
        assertThatThrownBy(() -> uint256Type.writeSlice(blockBuilder, almostCorrectSlice))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UINT256 length should be 32 bytes");

        // 测试33字节（多1字节）
        byte[] slightlyTooLongData = new byte[33];
        Slice slightlyTooLongSlice = Slices.wrappedBuffer(slightlyTooLongData);
        assertThatThrownBy(() -> uint256Type.writeSlice(blockBuilder, slightlyTooLongSlice))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UINT256 length should be 32 bytes");
    }

    @Test
    public void testBlockBuilderCapacity()
    {
        // 测试不为32字节抛出错误
        assertThatThrownBy(() -> uint256Type.createBlockBuilder(null, 10, 16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UINT256 block entry length should be 32 bytes");

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
    public void testAppendToWithNulls()
    {
        // 测试包含null值的appendTo
        BlockBuilder sourceBuilder = uint256Type.createBlockBuilder(null, 3);

        byte[] testData = new byte[32];
        testData[0] = 42;
        uint256Type.writeSlice(sourceBuilder, Slices.wrappedBuffer(testData));
        sourceBuilder.appendNull();

        testData[0] = 24;
        uint256Type.writeSlice(sourceBuilder, Slices.wrappedBuffer(testData));

        Block sourceBlock = sourceBuilder.build();

        BlockBuilder targetBuilder = uint256Type.createBlockBuilder(null, 3);
        for (int i = 0; i < 3; i++) {
            uint256Type.appendTo(sourceBlock, i, targetBuilder);
        }

        Block targetBlock = targetBuilder.build();
        assertThat(targetBlock.getPositionCount()).isEqualTo(3);
        assertThat(targetBlock.isNull(0)).isFalse();
        assertThat(targetBlock.isNull(1)).isTrue();
        assertThat(targetBlock.isNull(2)).isFalse();

        assertThat(uint256Type.getSlice(targetBlock, 0).getByte(0)).isEqualTo((byte) 42);
        assertThat(uint256Type.getSlice(targetBlock, 2).getByte(0)).isEqualTo((byte) 24);
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

    @Test
    public void testVarcharCastRoundtrip()
    {
        // '' is not valid
        assertThatThrownBy(() -> UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice("")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid UINT256 value:");
        // '15' -> 15
        Slice v = UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice("15"));
        String hex = UInt256Operators.castFromUint256ToVarchar(v).toStringUtf8();
        assertThat(hex).endsWith("15");

        // 长度校验
        assertThatThrownBy(() -> UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice("115792089237316195423570985008687907853269984665640564039457584007913129639936")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("uint256 value out of range");
        // 非法字符
        assertThatThrownBy(() -> UInt256Operators.castFromVarcharToUint256(Slices.utf8Slice("123F")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid UINT256 value: ");
    }

    @Test
    public void testAddition()
    {
        // Test: 0 + 0 = 0
        Slice zero = uint256FromHex("0");
        Slice result = UInt256Operators.add(zero, zero);
        assertThat("0").isEqualTo(toDecimal(result));

        // Test: 1 + 1 = 2
        Slice one = uint256FromHex("1");
        Slice two = uint256FromHex("2");
        result = UInt256Operators.add(one, one);
        assertThat(toDecimal(two)).isEqualTo(toDecimal(result));

        // Test: 255 + 1 = 256 (carry propagation)
        Slice ff = uint256FromHex("ff");
        Slice hundred = uint256FromHex("100");
        result = UInt256Operators.add(ff, one);
        assertThat(toDecimal(hundred)).isEqualTo(toDecimal(result));

        // Test: large numbers
        Slice large1 = uint256FromHex("123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        Slice large2 = uint256FromHex("fedcba9876543210fedcba9876543210fedcba9876543210fedcba987654321");
        result = UInt256Operators.add(large1, large2);
        // Expected: 1111111111111110111111111111111011111111111111101111111111111110
        assertThat(toDecimal(result)).isEqualTo("7719472615821079688627630598525846426041927187580766056379662137891363033360");
    }

    @Test
    public void testAdditionOverflow()
    {
        // Test overflow: MAX_UINT256 + 1
        Slice maxUint256 = uint256FromHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        Slice one = uint256FromHex("1");
        Slice zero = uint256FromHex("0");

        assertThatThrownBy(() -> UInt256Operators.add(maxUint256, one)).isInstanceOf(TrinoException.class)
                .hasMessageContaining("uint256 addition overflow");
        assertThat(toDecimal(UInt256Operators.add(maxUint256, zero))).isEqualTo(toDecimal(maxUint256));
    }

    @Test
    public void testAdditionCarryPropagation()
    {
        // Test multiple byte carry propagation
        Slice value1 = uint256FromHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00");
        Slice value2 = uint256FromHex("ff");
        Slice expected1 = uint256FromHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        Slice result1 = UInt256Operators.add(value1, value2);
        assertThat(toDecimal(expected1)).isEqualTo(toDecimal(result1));

        Slice value3 = uint256FromHex("1fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        Slice value4 = uint256FromHex("1");
        Slice unexpected = uint256FromHex("2fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        Slice expected2 = uint256FromHex("2000000000000000000000000000000000000000000000000000000000000000");

        Slice result2 = UInt256Operators.add(value3, value4);
        assertThat(toDecimal(expected2)).isEqualTo(toDecimal(result2)).isNotEqualTo(toDecimal(unexpected));
    }

    private Slice uint256FromHex(String hex)
    {
        if (hex.length() > 64) {
            throw new IllegalArgumentException("Hex string too long for UINT256: " + hex);
        }
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

    private String toDecimal(Slice slice)
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
        assertThat(subHex).endsWith("255");

        // multiply: 0x02 * 0x03 = 0x06
        Slice m2 = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x02}));
        Slice m3 = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x03}));
        Slice mul = UInt256Operators.multiply(m2, m3);
        assertThat(UInt256Operators.castFromUint256ToVarchar(mul).toStringUtf8()).endsWith("6");

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
        assertThat(UInt256Operators.castFromUint256ToVarchar(div).toStringUtf8()).endsWith("4");

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
        assertThat(UInt256Operators.castFromUint256ToVarchar(and).toStringUtf8()).endsWith("0");

        // or => 0xFF
        Slice or = UInt256Operators.bitwiseOr(fzero, zerof);
        assertThat(UInt256Operators.castFromUint256ToVarchar(or).toStringUtf8()).endsWith("255");

        // xor => 0xFF
        Slice xor = UInt256Operators.bitwiseXor(fzero, zerof);
        assertThat(UInt256Operators.castFromUint256ToVarchar(xor).toStringUtf8()).endsWith("255");

        // not(~0x00ff)
        Slice v00ff = UInt256Operators.castFromVarbinaryToUint256(Slices.wrappedBuffer(new byte[] {0x00, (byte) 0xFF}));
        Slice not = UInt256Operators.bitwiseNot(v00ff);
        String notHex = toHex(UInt256Operators.castFromUint256ToVarbinary(not).byteArray());

        assertThat(notHex).isEqualTo("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00");
    }

    String toHex(byte[] bytes)
    {
        return new BigInteger(1, bytes).toString(16);
    }
}
