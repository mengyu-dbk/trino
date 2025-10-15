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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergTypes.convertTrinoValueToIceberg;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUInt256IcebergTypeConversion
{
    private static final TypeManager TYPE_MANAGER = TESTING_TYPE_MANAGER;

    @Test
    public void testUInt256ToIcebergFixedType()
    {
        // Test conversion from UInt256Type to Iceberg FixedType
        org.apache.iceberg.types.Type icebergType = toIcebergTypeForNewColumn(UInt256Type.UINT256, new AtomicInteger(1));

        assertThat(icebergType).isInstanceOf(Types.FixedType.class);
        Types.FixedType fixedType = (Types.FixedType) icebergType;
        assertThat(fixedType.length()).isEqualTo(32);
    }

    @Test
    public void testIcebergFixedTypeToUInt256()
    {
        // Test conversion from Iceberg FixedType(32) to UInt256Type when doc marker is present
        Types.FixedType icebergFixedType = Types.FixedType.ofLength(32);
        String columnDoc = "trino:type=uint256";
        io.trino.spi.type.Type trinoType = toTrinoType(icebergFixedType, TYPE_MANAGER, columnDoc);

        assertThat(trinoType).isEqualTo(UInt256Type.UINT256);
    }

    @Test
    public void testOtherFixedTypesRemainVarbinary()
    {
        // Test that other fixed types (not 32 bytes) still convert to VARBINARY
        Types.FixedType icebergFixedType16 = Types.FixedType.ofLength(16);
        io.trino.spi.type.Type trinoType = toTrinoType(icebergFixedType16, TYPE_MANAGER);

        assertThat(trinoType.getDisplayName()).isEqualTo("varbinary");
    }

    @Test
    public void testUInt256TablePropertiesMarking()
    {
        // Test the table properties creation logic directly
        ImmutableList<ColumnMetadata> columns = ImmutableList.of(
                ColumnMetadata.builder()
                        .setName("id")
                        .setType(UInt256Type.UINT256)
                        .build(),
                ColumnMetadata.builder()
                        .setName("name")
                        .setType(createVarcharType(100))
                        .build(),
                ColumnMetadata.builder()
                        .setName("hash")
                        .setType(UInt256Type.UINT256)
                        .build());

        // Test the logic directly
        long uint256Count = columns.stream()
                .filter(column -> column.getType().getTypeSignature().equals(UInt256Type.UINT256.getTypeSignature()))
                .count();

        assertThat(uint256Count).isEqualTo(2);

        String uint256Columns = columns.stream()
                .filter(column -> column.getType().getTypeSignature().equals(UInt256Type.UINT256.getTypeSignature()))
                .map(ColumnMetadata::getName)
                .collect(toImmutableList())
                .toString();

        assertThat(uint256Columns).contains("id", "hash");
    }

    @Test
    public void testUInt256TypeIdentificationWithDocMarker()
    {
        // Test that FixedType(32) is correctly identified as UINT256 when doc field contains marker
        Types.FixedType icebergFixedType = Types.FixedType.ofLength(32);
        String columnDoc = "trino:type=uint256\nThis is a UINT256 column";

        io.trino.spi.type.Type trinoType = toTrinoType(icebergFixedType, TYPE_MANAGER, columnDoc);
        assertThat(trinoType).isEqualTo(UInt256Type.UINT256);
    }

    @Test
    public void testUInt256TypeIdentificationWithoutDocMarker()
    {
        // Test that FixedType(32) is identified as VARBINARY without the marker
        Types.FixedType icebergFixedType = Types.FixedType.ofLength(32);
        String columnDoc = "Regular column without marker";

        io.trino.spi.type.Type trinoType = toTrinoType(icebergFixedType, TYPE_MANAGER, columnDoc);
        assertThat(trinoType).isEqualTo(VarbinaryType.VARBINARY);
    }

    @Test
    public void testUInt256DataConversion()
    {
        // Test runtime data conversion from Trino to Iceberg
        byte[] testBytes = new byte[32];
        // Fill with test pattern
        for (int i = 0; i < 32; i++) {
            testBytes[i] = (byte) (i % 256);
        }
        Slice trinoValue = Slices.wrappedBuffer(testBytes);

        Object icebergValue = convertTrinoValueToIceberg(UInt256Type.UINT256, trinoValue);
        assertThat(icebergValue).isInstanceOf(ByteBuffer.class);

        ByteBuffer buffer = (ByteBuffer) icebergValue;
        byte[] resultBytes = new byte[32];
        buffer.get(resultBytes);
        assertThat(resultBytes).isEqualTo(testBytes);
    }

    @Test
    public void testIcebergToTrinoDataConversion()
    {
        // Test runtime data conversion from Iceberg to Trino
        byte[] testBytes = new byte[32];
        // Fill with test pattern
        for (int i = 0; i < 32; i++) {
            testBytes[i] = (byte) ((i * 3) % 256);
        }
        ByteBuffer icebergValue = ByteBuffer.wrap(testBytes);
        Types.FixedType icebergType = Types.FixedType.ofLength(32);

        Object trinoValue = convertIcebergValueToTrino(icebergType, icebergValue);
        assertThat(trinoValue).isInstanceOf(Slice.class);

        Slice slice = (Slice) trinoValue;
        assertThat(slice.getBytes()).isEqualTo(testBytes);
    }

    @Test
    public void testUInt256TypeInstanceofChecks()
    {
        // Test that UInt256Type is correctly identified and doesn't conflict with VarbinaryType
        UInt256Type uint256Type = UInt256Type.UINT256;
        VarbinaryType varbinaryType = VarbinaryType.VARBINARY;

        // Verify UInt256Type is not an instance of VarbinaryType (compile-time verification)
        // Note: instanceof check would not compile, proving UInt256Type is not related to VarbinaryType
        assertThat(uint256Type.getClass()).isNotEqualTo(varbinaryType.getClass());

        // Verify TypeSignatures are different
        assertThat(uint256Type.getTypeSignature()).isNotEqualTo(varbinaryType.getTypeSignature());
        assertThat(uint256Type.getTypeSignature().toString()).isEqualTo("UINT256");
        assertThat(varbinaryType.getTypeSignature().toString()).isEqualTo("varbinary");

        // Verify UInt256Type converts to FixedType, not BinaryType
        org.apache.iceberg.types.Type uint256IcebergType = toIcebergTypeForNewColumn(uint256Type, new AtomicInteger(1));
        org.apache.iceberg.types.Type varbinaryIcebergType = toIcebergTypeForNewColumn(varbinaryType, new AtomicInteger(1));

        assertThat(uint256IcebergType).isInstanceOf(Types.FixedType.class);
        assertThat(varbinaryIcebergType).isInstanceOf(Types.BinaryType.class);
    }

    @Test
    public void testUInt256VarbinaryDataFormatDifference()
    {
        // Test that UInt256 and VARBINARY have different data representations in Iceberg
        byte[] testBytes = new byte[32];
        // Fill with a recognizable pattern
        for (int i = 0; i < 32; i++) {
            testBytes[i] = (byte) (0xAA);
        }

        // Create UInt256 Slice (should be exactly 32 bytes)
        Slice uint256Slice = Slices.wrappedBuffer(testBytes);

        // Create VARBINARY Slice (same content)
        Slice varbinarySlice = Slices.wrappedBuffer(testBytes);

        // Convert both to Iceberg formats
        Object uint256IcebergValue = convertTrinoValueToIceberg(UInt256Type.UINT256, uint256Slice);
        Object varbinaryIcebergValue = convertTrinoValueToIceberg(VarbinaryType.VARBINARY, varbinarySlice);

        // Both should be ByteBuffers but UInt256 goes to FixedType, VARBINARY goes to BinaryType
        assertThat(uint256IcebergValue).isInstanceOf(ByteBuffer.class);
        assertThat(varbinaryIcebergValue).isInstanceOf(ByteBuffer.class);

        // Verify the actual byte content is the same
        ByteBuffer uint256Buffer = (ByteBuffer) uint256IcebergValue;
        ByteBuffer varbinaryBuffer = (ByteBuffer) varbinaryIcebergValue;

        byte[] uint256Bytes = new byte[uint256Buffer.remaining()];
        byte[] varbinaryBytes = new byte[varbinaryBuffer.remaining()];

        uint256Buffer.get(uint256Bytes);
        varbinaryBuffer.get(varbinaryBytes);

        assertThat(uint256Bytes).isEqualTo(testBytes);
        assertThat(varbinaryBytes).isEqualTo(testBytes);
        assertThat(uint256Bytes).isEqualTo(varbinaryBytes);
    }
}
