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

import io.trino.plugin.uint256.type.UInt256Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
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
        // Test conversion from Iceberg FixedType(32) to UInt256Type
        Types.FixedType icebergFixedType = Types.FixedType.ofLength(32);
        io.trino.spi.type.Type trinoType = toTrinoType(icebergFixedType, TYPE_MANAGER);

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
}
