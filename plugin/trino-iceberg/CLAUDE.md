# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Trino Iceberg 连接器概述

Trino Iceberg 连接器是一个功能完整的插件，用于连接和查询 Apache Iceberg 表格式。该连接器支持多种文件格式（Parquet、ORC、Avro）、多种目录实现（Hive Metastore、Glue、JDBC、Nessie、Snowflake、REST），并提供完整的表管理功能，包括表创建、数据插入、更新、删除、合并等操作。

### 核心架构组件

- **插件入口**: `IcebergPlugin` - 注册连接器工厂和聚合函数
- **连接器工厂**: `IcebergConnectorFactory` - 创建连接器实例，配置依赖注入模块
- **元数据管理**: `IcebergMetadata` - 核心元数据接口，处理表操作、schema 管理、查询计划
- **类型转换**: `TypeConverter` - Trino 和 Iceberg 类型系统之间的双向转换
- **数据序列化**: `IcebergTypes` - 运行时数据值在 Trino 和 Iceberg 表示之间的转换
- **数据写入**: 多个文件格式写入器（`IcebergParquetFileWriter`、`IcebergOrcFileWriter`、`IcebergAvroFileWriter`）
- **目录支持**: 位于 `catalog/` 包下的多种目录实现

### 开发和构建命令

#### 构建命令
```bash
# 编译整个项目
mvn clean compile

# 运行默认测试（排除云服务和 Minio/Avro 测试）
mvn test

# 运行 Minio 和 Avro 相关测试
mvn test -Pminio-and-avro

# 运行云服务相关测试（需要 AWS 凭证）
mvn test -Pcloud-tests

# 运行故障恢复测试
mvn test -Pfte-tests

# 打包插件
mvn package
```

#### 单元测试
```bash
# 运行单个测试类
mvn test -Dtest=TestIcebergMetadata

# 运行测试方法
mvn test -Dtest=TestIcebergMetadata#testCreateTable
```

### 新增类型支持写入 Iceberg 的改动要求

要为 Trino Iceberg 连接器添加新类型支持，需要在以下关键位置进行修改：

#### 1. 类型映射和转换 (`TypeConverter.java:119-255`)

**必须修改的方法：**
- `toTrinoType()` - 添加从 Iceberg 类型到 Trino 类型的映射
- `toIcebergTypeInternal()` - 添加从 Trino 类型到 Iceberg 类型的映射

**示例修改位置：**
```java
// 在 toTrinoType() switch 语句中添加新的 case
case NEW_TYPE_ID:
    return NewTrinoType.INSTANCE;

// 在 toIcebergTypeInternal() 中添加新的 if 条件
if (type instanceof NewTrinoType) {
    return NewIcebergType.get();
}
```

#### 2. 运行时数据转换 (`IcebergTypes.java:66-194`)

**必须修改的方法：**
- `convertTrinoValueToIceberg()` - 添加 Trino 运行时值到 Iceberg 值的转换
- `convertIcebergValueToTrino()` - 添加 Iceberg 值到 Trino 运行时值的转换

**关键考虑：**
- 确保数据序列化格式兼容
- 处理 null 值
- 考虑精度和范围限制
- 添加适当的类型检查和异常处理

#### 3. 文件格式写入器支持

**Parquet 支持 (`IcebergParquetFileWriter.java`)**:
- 确保 Parquet 库支持新类型
- 可能需要修改 `IcebergParquetColumnIOConverter.java` 中的列转换逻辑

**ORC 支持 (`IcebergOrcFileWriter.java`)**:
- 确保 ORC 库支持新类型
- 检查 Trino ORC 模块中的类型映射

**Avro 支持 (`IcebergAvroFileWriter.java`)**:
- 修改 `IcebergAvroDataConversion.java` 中的数据转换逻辑
- 确保 Avro schema 生成正确

#### 4. 测试覆盖

**必须添加的测试：**
- 类型转换测试（Trino ↔ Iceberg）
- 数据读写测试（所有支持的文件格式）
- 边界值和异常情况测试
- 与不同目录实现的兼容性测试

**测试文件位置：**
- `src/test/java/io/trino/plugin/iceberg/TestTypeConverter.java`
- `src/test/java/io/trino/plugin/iceberg/TestIcebergTypes.java`
- 格式特定的测试文件

#### 5. 潜在的额外修改点

**分区和排序支持：**
- `PartitionTransforms.java` - 如果新类型需要支持分区转换
- `SortFieldUtils.java` - 如果新类型需要支持排序

**统计信息收集：**
- `TableStatisticsWriter.java` - 添加新类型的统计信息收集
- 各文件格式的 metrics 计算逻辑

**表达式下推：**
- `ExpressionConverter.java` - 如果需要支持谓词下推优化

### 开发注意事项

1. **向后兼容性**: 确保新类型支持不破坏现有功能
2. **性能考虑**: 新类型转换应该高效，避免不必要的内存分配
3. **错误处理**: 提供清晰的错误消息，使用适当的 `IcebergErrorCode`
4. **文档更新**: 更新相关文档说明新类型的支持情况和限制
5. **Iceberg 版本兼容**: 确保新类型与项目使用的 Iceberg 版本兼容

### 代码风格和约定

- 遵循现有的 Java 编码风格
- 使用 `requireNonNull()` 进行参数验证
- 优先使用 Guava 集合类型（如 `ImmutableList`、`ImmutableMap`）
- 异常处理使用 `TrinoException` 包装，并指定适当的错误码
- 日志使用 Airlift Logger 框架