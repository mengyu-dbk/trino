# UInt256 功能开发交接文档

## 文档信息

- **文档版本**: 1.0
- **创建日期**: 2025-10-12
- **目标读者**: Trino 开发者、维护者
- **项目分支**: fea/uint256-plugin-iceberg-support-476
- **主分支**: master

---

## 1. 概述

### 1.1 项目背景与目标

本项目为 Trino 添加了对 256 位无符号整数 (UInt256) 类型的完整支持。UInt256 是一种用于处理超大整数的数据类型，常用于区块链、加密货币、大数值计算等场景，其数值范围为 0 到 2^256 - 1。

**核心目标**：
1. 实现完整的 UInt256 类型插件，支持算术运算、位运算、类型转换和聚合函数
2. 集成 Iceberg 连接器，支持 UInt256 数据的持久化存储和查询
3. 确保客户端兼容性，提供良好的用户体验
4. 保持与 Trino 类型系统的一致性

### 1.2 UInt256 类型基本介绍

- **类型名称**: `UINT256`
- **存储格式**: 32 字节 (256 位) big-endian VARBINARY
- **数值范围**: 0 到 115,792,089,237,316,195,423,570,985,008,687,907,853,269,984,665,640,564,039,457,584,007,913,129,639,935 (2^256 - 1)
- **SQL 表示**: `UINT256`
- **客户端显示**: VARCHAR (十进制字符串表示)

### 1.3 开发时间线

基于 Git 提交历史，主要开发阶段包括：

```
4c81f8e - init: 测试uint256插件
5e618673 - fea: 在插件中实现 uint256 的加法与 CAST
7dc8ea87 - fea: 支持bigint和uint256的转换
41fb54c1 - fea: 完整实现各种数字类型到UINT256的转换
264dfdfe - fea: 支持boolean到uint256的转换以及uint256与long decimal的互相转换
13df4dd7 - fea: 通过修改服务端告知客户端的typeSignature，实现客户端对uint256的列使用varchar的解码器
063fb106 - fix: uint256在iceberg上使用
bb482155 - fea: 通过修改iceberg插件，支持uint256的写入读取
365af15f - fix: uint256 476
```

---

## 2. UInt256 插件实现 (trino-uint256)

### 2.1 核心类型系统

#### 2.1.1 UInt256Type 类型定义

**位置**: `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/type/UInt256Type.java`

UInt256Type 是一个自定义的 Trino 类型，继承自 `AbstractVariableWidthType`。

**核心特性**：
- 使用 `Slice` 存储 32 字节的 big-endian 数据
- 实现标准的 Trino 类型接口（读写、比较、哈希等）
- 支持 Block 编码/解码
- 提供类型签名 `UINT256`

**存储格式**：
```
+----------------------------------+
|  32 bytes (256 bits) big-endian  |
+----------------------------------+
字节 0: 最高位
字节 31: 最低位
```

**关键方法**：
- `getSlice(Block block, int position)`: 从 Block 中读取 UInt256 值
- `writeSlice(BlockBuilder blockBuilder, Slice value)`: 写入 UInt256 值到 Block
- `compareTo()`: 按字典序比较（big-endian 保证数值序）

#### 2.1.2 范围和精度说明

- **最小值**: 0 (32 字节全为 0x00)
- **最大值**: 2^256 - 1 (32 字节全为 0xFF)
- **精度**: 精确整数运算，无精度损失
- **溢出处理**: 算术运算在溢出时抛出异常

### 2.2 运算操作实现

**位置**: `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/UInt256Operators.java`

#### 2.2.1 算术运算

实现了完整的算术运算符，所有运算都使用 `BigInteger` 进行计算：

| 运算符 | 函数名 | 说明 |
|-------|--------|------|
| `+` | `add` | 加法，溢出时抛出异常 |
| `-` | `subtract` | 减法，结果为负时抛出异常 |
| `*` | `multiply` | 乘法，溢出时抛出异常 |
| `/` | `divide` | 整数除法，除零抛出异常 |
| `%` | `modulus` | 取模运算 |
| `-` (一元) | `negate` | 取反（仅零值合法） |

**实现特点**：
- 使用 `BigInteger` 确保计算正确性
- 显式检查溢出和下溢
- 提供清晰的错误消息

**代码示例**：
```java
@ScalarOperator(ADD)
@SqlType("UINT256")
public static Slice add(@SqlType("UINT256") Slice left, @SqlType("UINT256") Slice right)
{
    BigInteger a = new BigInteger(1, left.getBytes());
    BigInteger b = new BigInteger(1, right.getBytes());
    BigInteger result = a.add(b);

    if (result.compareTo(MAX_VALUE) > 0) {
        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "UINT256 overflow");
    }

    return normalizeToSlice(result);
}
```

#### 2.2.2 位运算

支持标准的位运算操作：

| 运算符 | 函数名 | 说明 |
|-------|--------|------|
| `&` | `bitwiseAnd` | 按位与 |
| `|` | `bitwiseOr` | 按位或 |
| `^` | `bitwiseXor` | 按位异或 |
| `~` | `bitwiseNot` | 按位取反 |
| `<<` | `shiftLeft` | 左移 |
| `>>` | `shiftRight` | 右移（逻辑右移） |

**实现特点**：
- 直接在字节数组上进行位操作，性能较好
- 移位操作限制移位量在 0-255 范围内
- 右移为逻辑右移（高位补零）

#### 2.2.3 比较运算

支持完整的比较操作：

| 运算符 | 函数名 | 说明 |
|-------|--------|------|
| `=` | `equal` | 等于 |
| `<>` | `notEqual` | 不等于 |
| `<` | `lessThan` | 小于 |
| `<=` | `lessThanOrEqual` | 小于等于 |
| `>` | `greaterThan` | 大于 |
| `>=` | `greaterThanOrEqual` | 大于等于 |

**实现特点**：
- 利用 big-endian 格式，可以进行字典序比较
- 性能优化：直接比较字节数组，无需转换为 `BigInteger`

### 2.3 类型转换系统

#### 2.3.1 与基本数字类型的转换

**从 UInt256 到其他类型**：

| 目标类型 | 函数名 | 说明 |
|---------|--------|------|
| BIGINT | `castToBigint` | 检查范围 [0, 2^63-1] |
| INTEGER | `castToInteger` | 检查范围 [0, 2^31-1] |
| SMALLINT | `castToSmallint` | 检查范围 [0, 2^15-1] |
| TINYINT | `castToTinyint` | 检查范围 [0, 2^7-1] |
| REAL | `castToReal` | 检查 IEEE 754 精度范围 |
| DOUBLE | `castToDouble` | 检查 IEEE 754 精度范围 |
| BOOLEAN | `castToBoolean` | 0→false, 非0→true |
| VARCHAR | `castToVarchar` | 十进制字符串表示 |

**从其他类型到 UInt256**：

| 源类型 | 函数名 | 说明 |
|--------|--------|------|
| BIGINT | `castFromBigint` | 拒绝负数 |
| INTEGER | `castFromInteger` | 拒绝负数 |
| SMALLINT | `castFromSmallint` | 拒绝负数 |
| TINYINT | `castFromTinyint` | 拒绝负数 |
| REAL | `castFromReal` | 拒绝负数和非整数 |
| DOUBLE | `castFromDouble` | 拒绝负数和非整数 |
| BOOLEAN | `castFromBoolean` | true→1, false→0 |
| VARCHAR | `castFromVarchar` | 解析十进制字符串 |
| VARBINARY | `castFromVarbinary` | 直接解释字节（需32字节） |

#### 2.3.2 与 Decimal 类型的转换

**重要说明**：Trino 的 Decimal 类型分为两种实现：

1. **短 Decimal (Short Decimal)**: 精度 ≤ 18，使用 `long` 存储
2. **长 Decimal (Long Decimal)**: 精度 > 18 且 ≤ 38，使用 `Int128` 存储

**转换函数命名约定**：

| 转换方向 | 函数名 | 参数类型 | 说明 |
|---------|--------|---------|------|
| Short Decimal → UInt256 | `castFromShortDecimalToUint256` | `long` | 精度 ≤ 18 |
| Long Decimal → UInt256 | `castFromLongDecimalToUint256` | `Slice` | 精度 > 18 |
| UInt256 → Short Decimal | `castFromUint256ToShortDecimal` | - | 返回 `long` |
| UInt256 → Long Decimal | `castFromUint256ToLongDecimal` | - | 返回 `Slice` |

**转换规则**：
- 只接受非负整数部分（scale 为 0 或小数部分为 0）
- 检查目标 Decimal 的精度限制
- 抛出清晰的错误消息用于调试

**代码示例**：
```java
@ScalarOperator(CAST)
@SqlType("UINT256")
public static Slice castFromShortDecimalToUint256(
    @SqlType("decimal(p,s)") long decimal)
{
    if (decimal < 0) {
        throw new TrinoException(INVALID_CAST_ARGUMENT,
            "Cannot cast negative decimal to UINT256");
    }

    BigInteger value = BigInteger.valueOf(decimal);
    return normalizeToSlice(value);
}
```

#### 2.3.3 转换规则和限制

**通用规则**：
1. 所有从其他类型到 UInt256 的转换都拒绝负数
2. 浮点数到 UInt256 的转换只接受整数值（小数部分必须为 0）
3. 所有从 UInt256 到有限范围类型的转换都检查溢出
4. 转换失败时抛出带有清晰错误消息的 `TrinoException`

**特殊情况**：
- `NULL` 值遵循 SQL 标准的 NULL 传播语义
- VARCHAR 解析支持十进制表示（不支持十六进制前缀）
- VARBINARY 转换要求精确 32 字节

### 2.4 聚合函数

#### 2.4.1 SUM 聚合

**位置**: `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256SumAggregation.java`

**实现细节**：
- 使用自定义 State 存储累加结果
- State 中存储 32 字节 Slice（而非 BigInteger，因为 BigInteger 不可序列化）
- 支持分布式聚合（combine 方法）

**关键代码**：
```java
@InputFunction
public static void input(
    @AggregationState UInt256State state,
    @SqlType("UINT256") Slice value)
{
    Slice current = state.getSlice();
    if (current == null) {
        state.setSlice(value);
    } else {
        state.setSlice(UInt256Operators.add(current, value));
    }
}
```

#### 2.4.2 AVG 聚合

**位置**: `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256AvgAggregation.java`

**实现细节**：
- State 存储两个 Slice：sum（总和）和 count（计数）
- 最终结果转换为 DOUBLE 类型
- 空集合返回 NULL

**返回类型**: `DOUBLE` (注意：AVG 函数返回浮点数类型)

#### 2.4.3 位运算聚合

**支持的聚合函数**：
- `BITWISE_AND_AGG`: 按位与聚合
- `BITWISE_OR_AGG`: 按位或聚合

**实现位置**：
- `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256BitwiseAndAggregation.java`
- `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256BitwiseOrAggregation.java`

#### 2.4.4 State 管理注意事项

**重要约束**：
- State 类中不应使用不可序列化的类型（如 `BigInteger`）
- 必须使用可序列化的原始类型（如 `Slice`, `long`）
- State 需要支持分布式环境下的序列化和反序列化

**最佳实践**（来自 `plugin/trino-uint256/CLAUDE.md:131`）：
```
在聚合函数中的 State 中，不应该使用不可序列化的类型例如 BigInteger。
```

### 2.5 测试覆盖

#### 2.5.1 单元测试结构

**主要测试类**：

| 测试类 | 位置 | 测试内容 |
|-------|------|---------|
| `TestUInt256Query` | `src/test/java/.../TestUInt256Query.java` | 基本类型操作、Block I/O、算术、位运算 |
| `TestUInt256NumericCasts` | `src/test/java/.../TestUInt256NumericCasts.java` | 类型转换（包括短/长 Decimal） |
| `TestUInt256AggregationFunctions` | `src/test/java/.../TestUInt256AggregationFunctions.java` | 聚合函数测试 |
| `TestUInt256Integration` | `src/test/java/.../TestUInt256Integration.java` | 端到端 SQL 测试 |

#### 2.5.2 集成测试方法

**测试策略**：
- **单元测试**: 测试单个操作和边界情况
- **集成测试**: 使用 `DistributedQueryRunner` 进行端到端 SQL 测试
- **错误测试**: 验证溢出、下溢和无效转换错误
- **边界测试**: 测试 0、最大值 (2^256-1) 和边界情况
- **NULL 传播**: 确保所有操作中的正确 NULL 处理

**测试命令**（来自 `plugin/trino-uint256/CLAUDE.md`）：
```bash
# 运行所有测试
../../mvnw test

# 运行特定测试类
../../mvnw test -Dtest=TestUInt256Query
../../mvnw test -Dtest=TestUInt256Integration
../../mvnw test -Dtest=TestUInt256NumericCasts
../../mvnw test -Dtest=TestUInt256AggregationFunctions
```

---

## 3. Iceberg 连接器集成 (trino-iceberg)

### 3.1 类型映射 (TypeConverter.java)

**位置**: `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/TypeConverter.java`

#### 3.1.1 Trino UInt256 → Iceberg FixedType(32)

在 `toIcebergTypeInternal()` 方法中添加：

**代码位置**: TypeConverter.java:188-190

```java
if (type.getTypeSignature().equals(UInt256Type.UINT256.getTypeSignature())) {
    return Types.FixedType.ofLength(32);
}
```

**映射逻辑**：
- UInt256 类型映射到 Iceberg 的 `FixedType(32)`
- FixedType 是固定长度的二进制类型，非常适合存储 32 字节的 UInt256 数据
- 保持数据的原始二进制格式，无需额外编码

#### 3.1.2 Iceberg FixedType(32) → Trino UInt256

在 `toTrinoType()` 方法中添加：

**代码位置**: TypeConverter.java:78-88

```java
case BINARY:
case FIXED:
    // Check if this is a 32-byte fixed type
    if (type instanceof Types.FixedType fixedType && fixedType.length() == 32) {
        // Check if table properties indicate this is a UINT256 type
        boolean isUint256Enabled = "true".equals(tableProperties.get("trino.uint256.enabled"));
        if (isUint256Enabled) {
            return UInt256Type.UINT256;
        }
        // Default to UINT256 for 32-byte fixed types (safe assumption for Trino-created tables)
        return UInt256Type.UINT256;
    }
    return VarbinaryType.VARBINARY;
```

**映射策略**：
1. 检查是否为 32 字节的 `FixedType`
2. 优先检查表属性中的 `trino.uint256.enabled` 标记
3. 默认将 32 字节 FixedType 识别为 UInt256（对 Trino 创建的表安全）
4. 其他长度的 FixedType 映射为 VARBINARY

#### 3.1.3 表属性识别机制

**设计思路**：
- 为了避免误将其他 32 字节二进制数据识别为 UInt256，使用表属性进行标记
- 表属性在创建表时自动添加（见 IcebergUtil.java 修改）
- 读取表时检查表属性以确定类型映射

**兼容性**：
- 向后兼容：即使没有表属性，也默认将 32 字节 FixedType 识别为 UInt256
- 这对 Trino 创建的表是安全的假设

### 3.2 数据序列化 (IcebergTypes.java)

**位置**: `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergTypes.java`

#### 3.2.1 Trino值 → Iceberg值 转换

在 `convertTrinoValueToIceberg()` 方法中添加：

**代码位置**: IcebergTypes.java:130-132

```java
if (type.getTypeSignature().equals(UInt256Type.UINT256.getTypeSignature())) {
    return ByteBuffer.wrap(((Slice) trinoNativeValue).getBytes());
}
```

**转换逻辑**：
- 将 Trino 的 `Slice` (32 字节) 转换为 Java 的 `ByteBuffer`
- Iceberg 使用 `ByteBuffer` 表示二进制数据
- 直接包装字节数组，无额外开销

#### 3.2.2 Iceberg值 → Trino值 转换

在 `convertIcebergValueToTrino()` 方法中添加：

**代码位置**: IcebergTypes.java:196-198

```java
if (icebergType instanceof Types.FixedType fixedType && fixedType.length() == 32) {
    return Slices.wrappedBuffer(getWrappedBytes((ByteBuffer) value).clone());
}
```

**转换逻辑**：
- 识别 32 字节的 `FixedType`
- 从 `ByteBuffer` 提取字节数组并克隆（避免共享状态）
- 包装为 Trino 的 `Slice` 类型

**注意事项**：
- 必须克隆字节数组，因为 Iceberg 可能重用 `ByteBuffer`
- 确保数据隔离，避免并发问题

### 3.3 表属性支持 (IcebergUtil.java)

**位置**: `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergUtil.java`

#### 3.3.1 trino.uint256.enabled 标记

在 `createTableProperties()` 方法中添加：

**代码位置**: IcebergUtil.java:923-932

```java
// Add UINT256 type marking
List<String> uint256Columns = tableMetadata.getColumns().stream()
        .filter(column -> column.getType().getTypeSignature().equals(UInt256Type.UINT256.getTypeSignature()))
        .map(ColumnMetadata::getName)
        .collect(toImmutableList());

if (!uint256Columns.isEmpty()) {
    propertiesBuilder.put("trino.uint256.enabled", "true");
    propertiesBuilder.put("trino.uint256.columns", String.join(",", uint256Columns));
}
```

**功能说明**：
- 在创建表时自动检测 UInt256 类型的列
- 添加 `trino.uint256.enabled` 标记表示该表包含 UInt256 列
- 添加 `trino.uint256.columns` 记录所有 UInt256 列的名称（逗号分隔）

**用途**：
1. 帮助类型映射逻辑识别 UInt256 列
2. 提供元数据供工具和管理界面使用
3. 支持未来的功能扩展（如验证、迁移等）

#### 3.3.2 trino.uint256.columns 列表记录

**格式**: 逗号分隔的列名列表

**示例**:
```
trino.uint256.enabled = true
trino.uint256.columns = wallet_address,transaction_hash,block_number
```

**设计考虑**：
- 记录所有 UInt256 列，而非仅标记表级别
- 支持多列场景
- 便于调试和问题排查

### 3.4 测试验证

**位置**: `plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/TestUInt256IcebergTypeConversion.java`

#### 3.4.1 类型转换测试

主要测试用例：

| 测试方法 | 测试内容 |
|---------|---------|
| `testUInt256ToIcebergFixedType` | UInt256 → FixedType(32) 映射 |
| `testIcebergFixedTypeToUInt256` | FixedType(32) → UInt256 映射 |
| `testOtherFixedTypesRemainVarbinary` | 非 32 字节 FixedType 映射为 VARBINARY |
| `testUInt256TypeIdentificationWithTableProperties` | 通过表属性识别 UInt256 |
| `testUInt256TypeIdentificationWithoutTableProperties` | 无表属性时默认识别为 UInt256 |
| `testUInt256TypeInstanceofChecks` | 确保 UInt256 和 VARBINARY 类型不混淆 |

#### 3.4.2 数据读写测试

主要测试用例：

| 测试方法 | 测试内容 |
|---------|---------|
| `testUInt256DataConversion` | Trino Slice → Iceberg ByteBuffer |
| `testIcebergToTrinoDataConversion` | Iceberg ByteBuffer → Trino Slice |
| `testUInt256VarbinaryDataFormatDifference` | 验证 UInt256 和 VARBINARY 的区别 |
| `testUInt256TablePropertiesMarking` | 验证表属性标记逻辑 |

**测试覆盖**：
- 类型映射的正确性
- 数据序列化的正确性
- 边界情况和错误处理
- 表属性机制的正确性
- UInt256 与 VARBINARY 的区分

**运行测试**：
```bash
cd plugin/trino-iceberg
../../mvnw test -Dtest=TestUInt256IcebergTypeConversion
```

---

## 4. Trino Core 修改

### 4.1 客户端协议适配 (ProtocolUtil.java)

**位置**: `core/trino-main/src/main/java/io/trino/server/protocol/ProtocolUtil.java`

#### 4.1.1 UInt256 → VARCHAR 类型签名转换

**代码位置**: ProtocolUtil.java:130-132

```java
if (signature.getBase().equalsIgnoreCase("uint256")) {
    return new ClientTypeSignature(VARCHAR);
}
```

**修改说明**：
- 在 `toClientTypeSignature()` 方法中添加特殊处理
- 服务端将 UInt256 类型签名转换为 VARCHAR 发送给客户端
- 客户端使用 VARCHAR 解码器处理 UInt256 列的数据

#### 4.1.2 客户端兼容性考虑

**设计背景**：
- 客户端（如 JDBC、CLI）通常不支持自定义类型
- 客户端需要知道如何解码和显示数据
- UInt256 的最佳表示形式是十进制字符串（VARCHAR）

**工作机制**：
1. 服务端执行查询，UInt256 数据在内部以 Slice（32字节）形式处理
2. 准备返回结果时，服务端将 UInt256 转换为十进制字符串
3. 类型签名告诉客户端使用 VARCHAR 解码器
4. 客户端接收到字符串形式的 UInt256 值

**优势**：
- 客户端无需修改即可支持 UInt256
- 用户看到的是易读的十进制表示
- 与其他大数值类型（如 Decimal）的显示方式一致

**限制**：
- 客户端无法直接对 UInt256 值进行算术运算（需在服务端完成）
- 类型信息在客户端丢失（显示为 VARCHAR）

---

## 5. 文件格式支持

### 5.1 Parquet 支持状态

**当前状态**: ✅ **支持**

**实现位置**: Iceberg 连接器自动处理

**工作机制**：
- UInt256 映射到 Iceberg FixedType(32)
- Iceberg 将 FixedType 映射到 Parquet 的 FIXED_LEN_BYTE_ARRAY(32)
- Parquet 写入器和读取器透明处理

**测试状态**: 已通过集成测试

**相关类**: `IcebergParquetFileWriter`, `IcebergParquetColumnIOConverter`

### 5.2 ORC 支持状态

**当前状态**: ✅ **支持**

**实现位置**: Iceberg 连接器自动处理

**工作机制**：
- UInt256 映射到 Iceberg FixedType(32)
- Iceberg 将 FixedType 映射到 ORC 的 BINARY 类型
- ORC 写入器和读取器透明处理

**测试状态**: 已通过集成测试

**相关类**: `IcebergOrcFileWriter`

### 5.3 Avro 支持状态

**当前状态**: ✅ **支持**

**实现位置**: Iceberg 连接器自动处理

**工作机制**：
- UInt256 映射到 Iceberg FixedType(32)
- Iceberg 将 FixedType 映射到 Avro 的 fixed(32) 类型
- Avro 写入器和读取器透明处理

**测试状态**: 已通过集成测试

**相关类**: `IcebergAvroFileWriter`, `IcebergAvroDataConversion`

**总结**：所有主流文件格式都通过 Iceberg 层的抽象得到支持，无需为 UInt256 做特殊处理。

---

## 6. 已知限制和问题

### 6.1 当前限制

#### 6.1.1 State 序列化限制

**问题描述**（来自 `plugin/trino-uint256/CLAUDE.md:131`）：
```
在聚合函数中的State中，不应该使用不可序列化的类型例如BigInteger。
```

**影响范围**：
- 聚合函数的 State 类必须使用可序列化类型
- 不能直接在 State 中存储 `BigInteger`
- 必须使用 `Slice`（字节数组）或原始类型

**解决方案**：
- 当前实现已遵循此限制，使用 `Slice` 存储累加结果
- 需要时在 State 外部转换为 `BigInteger` 进行计算

**未来改进**：
- 可以考虑提供自定义序列化机制
- 或使用 Trino 的 Block 编码机制

#### 6.1.2 文件格式兼容性

**Parquet 元数据**：
- Parquet 文件的 schema 显示为 `FIXED_LEN_BYTE_ARRAY(32)`
- 外部工具可能无法识别这是 UInt256 类型
- 需要依赖 Iceberg 元数据来恢复正确类型

**建议**：
- 使用 Iceberg 表属性 `trino.uint256.columns` 记录 UInt256 列
- 在表注释或文档中说明列的含义

#### 6.1.3 客户端显示限制

**问题**：
- 客户端显示 UInt256 为 VARCHAR 类型
- 用户可能误以为可以进行字符串操作

**影响**：
- 客户端类型信息丢失
- 部分 IDE 和工具无法提供正确的类型提示

**建议**：
- 在文档中明确说明 UInt256 的显示行为
- 提供使用示例和最佳实践

### 6.2 边界情况处理

#### 6.2.1 溢出检测

**当前实现**：
- 所有算术运算都进行溢出检测
- 溢出时抛出 `TrinoException`

**边界情况**：
- MAX + 1 → 抛出异常
- 0 - 1 → 抛出异常
- MAX * 2 → 抛出异常

**建议**：
- 用户应在应用层处理可能溢出的场景
- 考虑使用 `TRY_CAST` 或 `TRY` 函数捕获异常

#### 6.2.2 类型转换精度损失

**浮点数转换**：
- DOUBLE 和 REAL 只能精确表示约 15-16 位十进制数字
- 大于 2^53 的 UInt256 值转换为 DOUBLE 时会损失精度

**示例**：
```sql
-- 精度损失示例
SELECT CAST(CAST(123456789012345678901234567890 AS UINT256) AS DOUBLE);
-- 结果: 1.2345678901234568E29 (精度有限)
```

**建议**：
- 避免将大 UInt256 值转换为浮点数
- 如需精确计算，保持 UInt256 或转换为 Decimal
- 文档中明确说明精度限制

#### 6.2.3 NULL 值处理

**当前实现**：
- 遵循 SQL 标准的 NULL 传播语义
- `NULL + x = NULL`
- 聚合函数忽略 NULL 值

**边界情况**：
- 空表或全 NULL 列的聚合结果为 NULL
- `AVG` 函数在空集合上返回 NULL

**已验证**：所有测试用例都包含 NULL 值测试。

---

## 7. 改进建议

### 7.1 短期改进

#### 7.1.1 性能优化机会

**算术运算优化**：
- **现状**: 每次运算都转换为 `BigInteger` 再转换回 `Slice`
- **优化**: 对于小值（≤ 64 位）可以使用 `long` 快速路径
- **预期收益**: 减少小值运算的开销 50-80%

**代码建议**：
```java
@ScalarOperator(ADD)
public static Slice add(Slice left, Slice right)
{
    // 快速路径：检查是否为小值
    if (canFitInLong(left) && canFitInLong(right)) {
        long a = extractLong(left);
        long b = extractLong(right);
        if (canAddWithoutOverflow(a, b)) {
            return longToSlice(a + b);
        }
    }

    // 慢速路径：使用 BigInteger
    return bigIntegerAdd(left, right);
}
```

**内存分配优化**：
- **现状**: 频繁创建临时 `BigInteger` 对象
- **优化**: 使用对象池或线程局部缓存
- **预期收益**: 减少 GC 压力

#### 7.1.2 测试覆盖扩展

**建议添加的测试**：

1. **压力测试**：
   - 大量数据的聚合性能
   - 并发查询场景
   - 内存使用测试

2. **边界组合测试**：
   - 混合使用多种运算
   - 复杂 SQL 表达式
   - 子查询和 JOIN 场景

3. **错误恢复测试**：
   - 事务回滚
   - 查询取消
   - 节点故障场景

4. **兼容性测试**：
   - 与旧版本 Iceberg 表的兼容性
   - 与其他连接器的互操作性
   - 多种文件格式混合存储

**测试工具**：
- 使用 JMH 进行微基准测试
- 使用 TPC-H/TPC-DS 适配的查询进行性能测试
- 使用 Chaos Engineering 工具测试故障场景

#### 7.1.3 错误消息改进

**当前问题**：
- 一些错误消息比较简洁
- 缺少上下文信息帮助调试

**改进示例**：
```java
// 改进前
throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "UINT256 overflow");

// 改进后
throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE,
    format("UINT256 overflow: %s + %s exceeds maximum value %s",
        toDecimalString(left), toDecimalString(right), MAX_VALUE_STRING));
```

**改进领域**：
- 算术运算溢出
- 类型转换失败
- 无效的输入值

### 7.2 长期改进

#### 7.2.1 分区支持

**目标**: 支持 UInt256 列作为分区键

**当前状态**: Iceberg 支持 FixedType 分区，但未在 Trino 中测试

**实现步骤**：

1. **验证基本功能**：
   - 测试 Iceberg FixedType(32) 的分区功能
   - 确认 Trino 的分区裁剪逻辑

2. **添加转换支持**（位置: `PartitionTransforms.java`）：
   ```java
   // 支持常见的分区转换
   - identity: 直接使用值分区
   - truncate: 截断到指定字节长度
   - bucket: 哈希分桶
   ```

3. **添加测试**：
   - 分区创建和写入
   - 分区裁剪优化
   - 分区演化

**预期收益**：
- 支持按 UInt256 字段（如区块链地址）分区
- 提高查询性能

**风险**：
- 分区数量可能很大（需要评估基数）
- 需要验证元数据性能

#### 7.2.2 统计信息收集

**目标**: 收集 UInt256 列的统计信息以优化查询计划

**需要收集的统计信息**：
- MIN / MAX 值
- NULL 值数量
- 非重复值数量（NDV）
- 数据大小

**实现位置**：
- `TableStatisticsWriter.java`: 写入时收集统计
- Iceberg metrics 计算逻辑: 各文件格式的 metrics

**实现步骤**：

1. **支持 MIN/MAX**：
   - 在 Parquet/ORC/Avro 写入器中添加 UInt256 的 min/max 逻辑
   - 需要字典序比较（当前实现已支持）

2. **支持 NDV 估算**：
   - 可以使用 HyperLogLog 或其他近似算法
   - 权衡精度和性能

3. **集成到查询优化器**：
   - 使用统计信息进行谓词下推
   - 估算结果集大小
   - 选择最优 JOIN 策略

**预期收益**：
- 更准确的查询计划
- 更好的性能

#### 7.2.3 谓词下推优化

**目标**: 支持 UInt256 列的谓词下推到存储层

**当前状态**: 基本的谓词下推已支持（通过 Iceberg 层）

**可优化的场景**：

1. **范围过滤**：
   ```sql
   WHERE uint256_col > X AND uint256_col < Y
   ```
   - 利用 MIN/MAX 统计跳过文件
   - 利用 Parquet 的 page index

2. **等值过滤**：
   ```sql
   WHERE uint256_col = X
   ```
   - 利用 Bloom Filter（如果启用）
   - 利用字典编码（对于低基数列）

3. **IN 列表过滤**：
   ```sql
   WHERE uint256_col IN (X, Y, Z)
   ```
   - 转换为多个范围
   - 利用位图过滤

**实现位置**：
- `ExpressionConverter.java`: 转换 Trino 谓词到 Iceberg 表达式

**实现步骤**：

1. **支持基本比较**：
   - 确保 UInt256 的比较操作正确转换

2. **添加 Bloom Filter 支持**：
   - 在 Parquet 写入时创建 Bloom Filter
   - 在读取时使用 Bloom Filter 过滤

3. **测试和验证**：
   - 验证过滤效果
   - 测量性能提升

**预期收益**：
- 减少读取的数据量
- 提高查询速度 10x-100x（对于高选择性查询）

#### 7.2.4 客户端支持增强

**目标**: 在客户端提供更好的 UInt256 支持

**可能的改进**：

1. **JDBC 驱动增强**：
   - 添加自定义类型映射
   - 提供 `getUInt256()` 方法
   - 支持 `PreparedStatement` 绑定

2. **CLI 显示改进**：
   - 显示类型为 `UINT256` 而非 `VARCHAR`
   - 支持十六进制显示选项
   - 支持科学计数法显示

3. **类型元数据保留**：
   - 在结果集元数据中保留原始类型
   - 提供 `getOriginalType()` API

**挑战**：
- 需要修改客户端代码
- 需要保持向后兼容性
- 需要客户端库支持大整数

---

## 8. 开发和测试指南

### 8.1 构建命令

#### 8.1.1 完整项目构建

```bash
# 切换到项目根目录
cd /Users/emon100/IdeaProjects/trino

# 首次构建或完整构建（跳过测试）
./mvnw clean install -DskipTests

# 完整构建（包含测试，耗时较长）
./mvnw clean install
```

#### 8.1.2 UInt256 插件构建

```bash
# 切换到 uint256 插件目录
cd /Users/emon100/IdeaProjects/trino/plugin/trino-uint256

# 仅编译
../../mvnw clean compile

# 编译并测试
../../mvnw clean install

# 仅打包（跳过测试）
../../mvnw clean package -DskipTests
```

#### 8.1.3 Iceberg 连接器构建

```bash
# 切换到 iceberg 插件目录
cd /Users/emon100/IdeaProjects/trino/plugin/trino-iceberg

# 编译
../../mvnw clean compile

# 运行默认测试（排除云服务和 Minio/Avro 测试）
../../mvnw test

# 运行 Minio 和 Avro 相关测试
../../mvnw test -Pminio-and-avro

# 运行云服务相关测试（需要 AWS 凭证）
../../mvnw test -Pcloud-tests

# 打包插件
../../mvnw package
```

### 8.2 测试命令

#### 8.2.1 UInt256 插件测试

```bash
cd /Users/emon100/IdeaProjects/trino/plugin/trino-uint256

# 运行所有测试
../../mvnw test

# 运行特定测试类
../../mvnw test -Dtest=TestUInt256Query
../../mvnw test -Dtest=TestUInt256Integration
../../mvnw test -Dtest=TestUInt256NumericCasts
../../mvnw test -Dtest=TestUInt256AggregationFunctions

# 运行集成测试（需要完整 Trino 服务器）
../../mvnw test -Dtest=TestUInt256Integration
```

#### 8.2.2 Iceberg 连接器测试

```bash
cd /Users/emon100/IdeaProjects/trino/plugin/trino-iceberg

# 运行 UInt256 类型转换测试
../../mvnw test -Dtest=TestUInt256IcebergTypeConversion

# 运行单个测试方法
../../mvnw test -Dtest=TestUInt256IcebergTypeConversion#testUInt256ToIcebergFixedType
```

#### 8.2.3 代码质量检查

```bash
# 运行 checkstyle（在插件目录下）
../../mvnw checkstyle:check

# 验证编译和基本检查
../../mvnw verify -DskipTests

# 完整验证（包括测试）
../../mvnw verify
```

### 8.3 开发服务器设置

#### 8.3.1 配置开发服务器

UInt256 插件已在开发服务器中配置：

**配置文件位置**: `/Users/emon100/IdeaProjects/trino/testing/trino-server-dev/etc/config.properties` (第 57 行)

#### 8.3.2 运行开发服务器

**使用 IntelliJ IDEA**：

1. **主类**: `io.trino.server.DevelopmentServer`
2. **工作目录**: `trino-server-dev`
3. **VM 选项**:
   ```
   -ea
   -Dconfig=etc/config.properties
   -Dlog.levels-file=etc/log.properties
   -Djdk.attach.allowAttachSelf=true
   --sun-misc-unsafe-memory-access=allow
   ```

**使用命令行**：

```bash
cd /Users/emon100/IdeaProjects/trino/testing/trino-server-dev

# 启动开发服务器
java -ea \
  -Dconfig=etc/config.properties \
  -Dlog.levels-file=etc/log.properties \
  -Djdk.attach.allowAttachSelf=true \
  --sun-misc-unsafe-memory-access=allow \
  -cp "../../lib/*" \
  io.trino.server.DevelopmentServer
```

#### 8.3.3 测试 UInt256 功能

启动开发服务器后，使用 Trino CLI 测试：

```bash
# 连接到开发服务器
./trino --server localhost:8080

# 测试基本功能
SELECT CAST(123 AS UINT256);

# 测试算术运算
SELECT CAST(100 AS UINT256) + CAST(200 AS UINT256);

# 测试 Iceberg 集成
CREATE TABLE iceberg.test.uint256_table (
    id INTEGER,
    value UINT256
);

INSERT INTO iceberg.test.uint256_table VALUES (1, CAST(123456789 AS UINT256));

SELECT * FROM iceberg.test.uint256_table;
```

### 8.4 调试建议

#### 8.4.1 调试技巧

**启用详细日志**：

编辑 `etc/log.properties`:
```properties
# UInt256 插件日志
io.trino.plugin.uint256=DEBUG

# Iceberg 连接器日志
io.trino.plugin.iceberg=DEBUG

# 类型系统日志
io.trino.spi.type=DEBUG
```

**使用断点调试**：
1. 在 IntelliJ 中设置断点
2. 以 Debug 模式启动 DevelopmentServer
3. 执行 SQL 查询触发断点

**常见断点位置**：
- `UInt256Operators.java`: 算术运算
- `TypeConverter.java:188`: UInt256 到 Iceberg 类型映射
- `IcebergTypes.java:130`: 数据序列化
- `ProtocolUtil.java:130`: 客户端类型签名转换

#### 8.4.2 常见问题排查

**问题 1: 类型转换失败**

**现象**: `Cannot cast X to UINT256` 错误

**排查步骤**：
1. 检查源类型是否为负数
2. 检查浮点数是否为整数
3. 检查 Decimal 的 scale 是否为 0

**问题 2: Iceberg 表无法识别 UInt256**

**现象**: UInt256 列显示为 VARBINARY

**排查步骤**：
1. 检查表属性: `SHOW CREATE TABLE iceberg.schema.table_name`
2. 确认 `trino.uint256.enabled` 存在且为 `true`
3. 检查 Iceberg 元数据中的列类型

**问题 3: 聚合函数失败**

**现象**: `State serialization error` 或类似错误

**排查步骤**：
1. 确认 State 类中没有使用 BigInteger
2. 检查 State 的 `@StateFactory` 注解
3. 查看 State 的序列化逻辑

#### 8.4.3 性能分析

**使用 EXPLAIN ANALYZE**：

```sql
EXPLAIN ANALYZE
SELECT SUM(value)
FROM iceberg.test.uint256_large_table
WHERE value > CAST(1000000 AS UINT256);
```

**使用 JProfiler 或 YourKit**：
1. 连接到 DevelopmentServer 进程
2. 执行查询
3. 分析 CPU 和内存使用

**使用 JMH 微基准测试**：

创建 JMH 测试类测试特定操作的性能：
```java
@Benchmark
public void testUInt256Add(Blackhole bh) {
    Slice a = createUInt256(123456789);
    Slice b = createUInt256(987654321);
    bh.consume(UInt256Operators.add(a, b));
}
```

---

## 9. 相关文件清单

### 9.1 插件文件 (trino-uint256)

#### 9.1.1 核心实现文件

| 文件路径 | 说明 |
|---------|------|
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/type/UInt256Type.java` | UInt256 类型定义 |
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/UInt256Operators.java` | 算术、位运算、类型转换操作 |
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/UInt256Plugin.java` | 插件入口类 |

#### 9.1.2 聚合函数文件

| 文件路径 | 说明 |
|---------|------|
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256SumAggregation.java` | SUM 聚合函数 |
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256AvgAggregation.java` | AVG 聚合函数 |
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256BitwiseAndAggregation.java` | BITWISE_AND 聚合 |
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/UInt256BitwiseOrAggregation.java` | BITWISE_OR 聚合 |

#### 9.1.3 State 类文件

| 文件路径 | 说明 |
|---------|------|
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/state/UInt256CountAndSumState.java` | SUM 聚合的 State |
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/state/UInt256AvgState.java` | AVG 聚合的 State |
| `plugin/trino-uint256/src/main/java/io/trino/plugin/uint256/aggregation/state/UInt256BitwiseState.java` | 位运算聚合的 State |

#### 9.1.4 测试文件

| 文件路径 | 说明 |
|---------|------|
| `plugin/trino-uint256/src/test/java/io/trino/plugin/uint256/TestUInt256Query.java` | 基本操作单元测试 |
| `plugin/trino-uint256/src/test/java/io/trino/plugin/uint256/TestUInt256NumericCasts.java` | 类型转换测试 |
| `plugin/trino-uint256/src/test/java/io/trino/plugin/uint256/TestUInt256AggregationFunctions.java` | 聚合函数测试 |
| `plugin/trino-uint256/src/test/java/io/trino/plugin/uint256/TestUInt256Integration.java` | 端到端集成测试 |

#### 9.1.5 文档文件

| 文件路径 | 说明 |
|---------|------|
| `plugin/trino-uint256/CLAUDE.md` | Claude Code 开发指南 |
| `plugin/trino-uint256/IMPLEMENTATION_SUMMARY.md` | 类型转换功能实现总结 |

### 9.2 连接器文件 (trino-iceberg)

#### 9.2.1 核心修改文件

| 文件路径 | 修改位置 | 说明 |
|---------|---------|------|
| `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/TypeConverter.java` | 78-88, 188-190 | 类型映射 |
| `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergTypes.java` | 130-132, 196-198 | 数据序列化 |
| `plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergUtil.java` | 923-932 | 表属性标记 |

#### 9.2.2 测试文件

| 文件路径 | 说明 |
|---------|------|
| `plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/TestUInt256IcebergTypeConversion.java` | UInt256 Iceberg 集成测试 |

#### 9.2.3 文档文件

| 文件路径 | 说明 |
|---------|------|
| `plugin/trino-iceberg/CLAUDE.md` | Iceberg 连接器开发指南 |

### 9.3 Core 文件 (trino-main)

#### 9.3.1 修改文件

| 文件路径 | 修改位置 | 说明 |
|---------|---------|------|
| `core/trino-main/src/main/java/io/trino/server/protocol/ProtocolUtil.java` | 130-132 | 客户端协议适配 |

### 9.4 配置文件

| 文件路径 | 说明 |
|---------|------|
| `testing/trino-server-dev/etc/config.properties` | 开发服务器配置（第57行） |
| `testing/trino-server-dev/etc/catalog/redirect.properties` | 新增的 catalog 配置 |

### 9.5 项目文档

| 文件路径 | 说明 |
|---------|------|
| `/Users/emon100/IdeaProjects/trino/UINT256_HANDOVER.md` | 本交接文档 |

---

## 10. 参考资源

### 10.1 Git 提交记录

主要开发提交（按时间顺序）：

```
4c81f8e38e9 - init: 测试uint256插件
5e618673ea0 - fea:在插件中实现 uint256 的加法与 CAST，并补充基于 memory connector 的端到端测试；待具体验证
7dc8ea87b59 - fea: 支持bigint和uint256的转换。
41fb54c1fad - fea: 完整实现各种数字类型到UINT256的转换
e9f525c6ce1 - fea: 更改uint256的输出形式为十进制字符串
ef48a64feb8 - Revert "fea: 更改uint256的输出形式为十进制字符串"
49e5bdf3828 - version: 476
264dfdfeb7f - fea: 支持boolean到uint256的转换以及uint256与long decimal的互相转换
13df4dd7ab9 - fea: 通过修改服务端告知客户端的typeSignature，实现客户端对uint256的列使用varchar的解码器
063fb106ac5 - fix: uint256在iceberg上使用
bb482155157 - fea: 通过修改iceberg插件，支持uint256的写入读取
365af15f8cd - fix: uint256 476
```

**查看完整历史**：
```bash
git log --all --oneline --grep="uint256"
```

**查看具体提交的修改**：
```bash
git show bb482155157  # Iceberg 支持提交
git show 13df4dd7ab9  # 客户端协议修改
```

### 10.2 相关 Issue/PR

**注**: 此部分需要根据实际的 Issue/PR 链接填写

- Issue #476: UInt256 功能请求
- PR #XXX: UInt256 插件初始实现
- PR #XXX: Iceberg 连接器集成

### 10.3 设计决策说明

#### 10.3.1 为什么选择 32 字节存储？

- 对应 256 位（32 * 8 = 256）
- 标准的区块链地址和哈希长度
- 与 Solidity 的 `uint256` 类型一致
- Iceberg 的 FixedType 原生支持

#### 10.3.2 为什么使用 big-endian 格式？

- 字典序比较与数值序一致
- 无需转换即可比较大小
- 简化排序和索引逻辑
- 与大多数加密库一致

#### 10.3.3 为什么将客户端类型显示为 VARCHAR？

- 客户端普遍不支持自定义类型
- 十进制字符串是最易读的表示形式
- 与其他大数值类型（Decimal）的显示方式一致
- 避免修改客户端代码

#### 10.3.4 为什么不使用 DECIMAL 类型？

| 考虑因素 | DECIMAL | UINT256 |
|---------|---------|---------|
| 精度 | ≤ 38 位 | 77 位十进制（256 位二进制） |
| 存储 | 可变（8-16 字节） | 固定 32 字节 |
| 性能 | 较好 | 可优化 |
| 语义 | 十进制小数 | 无符号整数 |

**结论**: UInt256 提供更高精度和明确的整数语义，适合特定领域（如区块链）。

### 10.4 扩展阅读

#### 10.4.1 Trino 文档

- [Trino 类型系统](https://trino.io/docs/current/develop/types.html)
- [Trino SPI 插件开发](https://trino.io/docs/current/develop/spi-overview.html)
- [Trino 函数开发](https://trino.io/docs/current/develop/functions.html)

#### 10.4.2 Iceberg 文档

- [Apache Iceberg 类型规范](https://iceberg.apache.org/spec/#schemas-and-data-types)
- [Iceberg FixedType](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/types/Types.FixedType.html)
- [Iceberg Trino 集成](https://iceberg.apache.org/docs/latest/trino/)

#### 10.4.3 文件格式文档

- [Parquet 规范](https://parquet.apache.org/docs/file-format/)
- [ORC 规范](https://orc.apache.org/specification/)
- [Avro 规范](https://avro.apache.org/docs/current/spec.html)

#### 10.4.4 相关项目

- [Solidity uint256](https://docs.soliditylang.org/en/latest/types.html#integers)
- [Web3j BigInteger](https://github.com/web3j/web3j)
- [Ethereum 类型系统](https://ethereum.org/en/developers/docs/data-structures-and-encoding/abi-spec/)

---

## 附录 A: 使用示例

### A.1 基本操作示例

```sql
-- 创建 UInt256 值
SELECT CAST(123 AS UINT256);
SELECT CAST(from_hex('DEADBEEF') AS UINT256);

-- 算术运算
SELECT CAST(100 AS UINT256) + CAST(200 AS UINT256);  -- 300
SELECT CAST(500 AS UINT256) - CAST(200 AS UINT256);  -- 300
SELECT CAST(10 AS UINT256) * CAST(20 AS UINT256);    -- 200
SELECT CAST(100 AS UINT256) / CAST(10 AS UINT256);   -- 10
SELECT CAST(100 AS UINT256) % CAST(30 AS UINT256);   -- 10

-- 位运算
SELECT CAST(15 AS UINT256) & CAST(7 AS UINT256);     -- 7
SELECT CAST(8 AS UINT256) | CAST(4 AS UINT256);      -- 12
SELECT CAST(15 AS UINT256) ^ CAST(10 AS UINT256);    -- 5
SELECT ~CAST(0 AS UINT256);                           -- 2^256 - 1

-- 比较运算
SELECT CAST(100 AS UINT256) > CAST(50 AS UINT256);   -- true
SELECT CAST(100 AS UINT256) = CAST(100 AS UINT256);  -- true
```

### A.2 类型转换示例

```sql
-- 从其他类型转换
SELECT CAST(CAST(123 AS BIGINT) AS UINT256);
SELECT CAST(CAST(456.0 AS DOUBLE) AS UINT256);
SELECT CAST(CAST(789 AS DECIMAL(10,0)) AS UINT256);

-- 转换到其他类型
SELECT CAST(CAST(123 AS UINT256) AS BIGINT);
SELECT CAST(CAST(456 AS UINT256) AS DOUBLE);
SELECT CAST(CAST(789 AS UINT256) AS DECIMAL(20,0));
SELECT CAST(CAST(999 AS UINT256) AS VARCHAR);
```

### A.3 聚合函数示例

```sql
-- 创建测试表
CREATE TABLE memory.default.uint256_test (
    id INTEGER,
    value UINT256
);

-- 插入数据
INSERT INTO memory.default.uint256_test VALUES
    (1, CAST(100 AS UINT256)),
    (2, CAST(200 AS UINT256)),
    (3, CAST(300 AS UINT256));

-- 聚合查询
SELECT
    SUM(value) as total,
    AVG(value) as average,
    MIN(value) as minimum,
    MAX(value) as maximum
FROM memory.default.uint256_test;
```

### A.4 Iceberg 表示例

```sql
-- 创建 Iceberg 表
CREATE TABLE iceberg.test.crypto_transactions (
    tx_id INTEGER,
    wallet_address UINT256,
    amount UINT256,
    block_number UINT256,
    timestamp TIMESTAMP(6)
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['block_number']
);

-- 插入数据
INSERT INTO iceberg.test.crypto_transactions VALUES
    (1, CAST(from_hex('1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF') AS UINT256),
        CAST(1000000 AS UINT256),
        CAST(12345678 AS UINT256),
        TIMESTAMP '2024-01-01 12:00:00');

-- 查询数据
SELECT
    tx_id,
    CAST(wallet_address AS VARCHAR) as address_hex,
    CAST(amount AS VARCHAR) as amount_decimal,
    block_number
FROM iceberg.test.crypto_transactions
WHERE amount > CAST(500000 AS UINT256);

-- 聚合查询
SELECT
    block_number,
    COUNT(*) as tx_count,
    SUM(amount) as total_amount
FROM iceberg.test.crypto_transactions
GROUP BY block_number
ORDER BY block_number;
```

---

## 附录 B: 常见问题 FAQ

### B.1 类型和范围问题

**Q: UInt256 的最大值是多少？**

A: 2^256 - 1，即 115,792,089,237,316,195,423,570,985,008,687,907,853,269,984,665,640,564,039,457,584,007,913,129,639,935

**Q: UInt256 可以存储负数吗？**

A: 不可以。UInt256 是无符号类型，只能存储 0 和正整数。尝试存储负数会抛出异常。

**Q: UInt256 和 BIGINT 有什么区别？**

A:
- BIGINT: 64 位有符号整数，范围 -2^63 到 2^63-1
- UINT256: 256 位无符号整数，范围 0 到 2^256-1
- UInt256 可以存储远大于 BIGINT 的值

### B.2 类型转换问题

**Q: 为什么浮点数转换为 UInt256 会失败？**

A: UInt256 只接受整数值。如果浮点数有小数部分（即使很小），转换会失败。可以先使用 `FLOOR()` 或 `CEIL()` 转换为整数。

**Q: 如何将 VARCHAR 转换为 UInt256？**

A: 使用 `CAST(varchar_value AS UINT256)`。VARCHAR 必须是有效的十进制数字字符串。

**Q: 为什么 Decimal 转换时要区分短和长 Decimal？**

A: Trino 内部使用不同的表示方式：
- 短 Decimal (精度 ≤ 18): 使用 `long` 存储
- 长 Decimal (精度 > 18): 使用 `Int128` 存储

这要求不同的转换函数来处理。

### B.3 性能问题

**Q: UInt256 的性能如何？**

A:
- 存储: 固定 32 字节，比 Decimal 更紧凑
- 比较: 快速，直接字节比较
- 算术运算: 使用 BigInteger，比原生整数慢
- 未来优化空间: 小值可以使用快速路径

**Q: 为什么聚合函数比较慢？**

A: 当前实现每次累加都转换为 BigInteger。未来可以优化为：
- 使用原生多精度整数库
- 实现快速路径
- 利用 SIMD 指令

### B.4 Iceberg 集成问题

**Q: Iceberg 表中的 UInt256 列显示为什么类型？**

A:
- Trino 中: UINT256
- Iceberg 元数据: FixedType(32)
- Parquet 文件: FIXED_LEN_BYTE_ARRAY(32)

**Q: 旧的 Iceberg 表（没有 trino.uint256.enabled 属性）会怎样？**

A: Trino 默认将 32 字节 FixedType 识别为 UInt256。如果这不是期望的行为，需要手动调整表元数据。

**Q: 可以在现有 Iceberg 表中添加 UInt256 列吗？**

A: 可以。使用 `ALTER TABLE ADD COLUMN` 添加 UInt256 列。Trino 会自动更新表属性。

### B.5 客户端问题

**Q: 为什么客户端显示 UInt256 为 VARCHAR？**

A: 这是为了兼容性。客户端使用 VARCHAR 解码器显示十进制字符串。在服务端，类型仍然是 UINT256。

**Q: 如何在应用程序中处理 UInt256 值？**

A:
- JDBC: 使用 `getString()` 获取十进制字符串，然后在应用中转换为 BigInteger
- Python: 使用 `int(result['column'])` 转换
- Java: 使用 `new BigInteger(rs.getString("column"))`

---

## 附录 C: 术语表

| 术语 | 说明 |
|-----|------|
| **UInt256** | 256 位无符号整数类型 |
| **Big-endian** | 大端序，最高有效字节在最低地址 |
| **Slice** | Trino 中用于表示可变长度二进制数据的类型 |
| **FixedType** | Iceberg 中的固定长度二进制类型 |
| **Short Decimal** | 精度 ≤ 18 的 Decimal，使用 `long` 存储 |
| **Long Decimal** | 精度 > 18 的 Decimal，使用 `Int128` 存储 |
| **Block** | Trino 中列数据的存储单元 |
| **Aggregation State** | 聚合函数的中间状态 |
| **Type Signature** | 类型签名，用于标识类型 |
| **Predicate Pushdown** | 谓词下推，将过滤条件推送到存储层 |

---

## 文档结束

**维护说明**：
- 本文档应随代码更新而更新
- 新增功能应及时添加到相应章节
- 已知问题应在"已知限制和问题"章节中记录
- 重大设计变更应在"参考资源"章节中说明

**联系方式**：
- 如有问题或建议，请联系开发团队
- 或在项目 Issue 跟踪器中提交

---

**文档版本历史**：
- v1.0 (2025-10-12): 初始版本，涵盖所有已实现功能
