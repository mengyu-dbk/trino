# UINT256 类型支持

## 概述
此文档为cursor生成，待人工校验。

## 特性

- **类型名称**: `UINT256`
- **底层存储**: `VARBINARY(32)` (32字节)
- **支持操作**: 比较、排序、读写
- **空值支持**: 是
- **范围**: 0 到 2^256 - 1

## 使用方法

### 创建表

```sql
-- 创建包含UINT256列的表
CREATE TABLE memory.default.test_table (
    id INTEGER,
    uint256_value uint256
);
```

### 插入数据

```sql
-- 使用十六进制字符串插入数据
INSERT INTO memory.default.test_table VALUES 
(1, X'0000000000000000000000000000000000000000000000000000000000000001'),
(2, X'0000000000000000000000000000000000000000000000000000000000000002');
```

### 查询数据

```sql
-- 基本查询
SELECT * FROM memory.default.test_table;

-- 比较操作
SELECT * FROM memory.default.test_table 
WHERE uint256_value > X'0000000000000000000000000000000000000000000000000000000000000001';

-- 排序
SELECT * FROM memory.default.test_table 
ORDER BY uint256_value;
```

## 技术实现

### 类型定义

`UInt256Type` 继承自 `AbstractVariableWidthType`，提供以下功能：

- 32字节固定长度存储
- 支持比较和排序操作
- 完整的读写操作支持
- 空值处理

### 注册方式

类型通过 `MemoryPlugin.getTypes()` 方法注册到 Trino 类型系统中。

## 注意事项

1. **存储大小**: 每个 UINT256 值占用 32 字节
2. **比较操作**: 支持所有标准比较操作符 (<, <=, =, >=, >, !=)
3. **排序**: 支持 ORDER BY 子句
4. **索引**: 可以创建包含 UINT256 列的索引

## 示例应用场景

- **区块链地址**: 存储以太坊地址等
- **大整数计算**: 需要处理超过 BIGINT 范围的数值
- **加密哈希**: 存储 SHA-256 等哈希值
- **科学计算**: 高精度数值计算

## 测试
当前测试是cursor生成的，还暂时不可用。
之后会运行测试以验证功能。

```bash
mvn test -Dtest=TestUInt256Type
mvn test -Dtest=TestUInt256Integration
```
