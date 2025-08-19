# UINT256 类型支持

## 概述
提供在 Trino 中使用 256 位无符号整数（uint256）的类型与函数。

## 特性
- 类型名称: `uint256`
- 底层表示: `VARBINARY(32)`（建议并默认规范化为大端32字节）
- 支持能力:
  - 比较、排序、读写
  - 算术：加法（`+`），溢出报错（NUMERIC_VALUE_OUT_OF_RANGE）
  - CAST：`varbinary ↔ uint256`
  - 便捷构造函数：`uint256(varbinary)`
- 空值支持: 与 SQL 一致
- 数值范围: 0 .. 2^256 - 1

说明：为保证排序/比较语义正确，推荐通过 `CAST(... AS uint256)` 或 `uint256(...)` 将值规范化为 32 字节大端表示；直接写入非 32 字节 varbinary 也能存储，但不保证排序一致性。

## 安装与注册
插件通过 `UInt256Plugin` 全局注册：
- `getTypes()` 注册类型 `uint256`
- `getFunctions()` 注册运算符与 CAST（包含加法、varbinary↔uint256、uint256(varbinary)）

## 使用方法

### 1) 创建表（使用 memory connector）
```sql
CREATE TABLE memory.default.uint256_test (
    id INTEGER,
    v  uint256
);
```

### 2) 插入数据（推荐 from_hex + CAST/构造函数）
```sql
-- 方式A：CAST(varbinary AS uint256)
INSERT INTO memory.default.uint256_test VALUES
(1, CAST(from_hex('01') AS uint256)),
(2, CAST(from_hex('02') AS uint256)),
(3, NULL);

-- 方式B：uint256(varbinary)
INSERT INTO memory.default.uint256_test VALUES (4, uint256(from_hex('FF')));
```

### 3) 查询/比较/排序
```sql
-- 基本查询
SELECT id, to_hex(CAST(v AS varbinary)) FROM memory.default.uint256_test ORDER BY id;

-- 比较与排序（大端 32 字节语义）
SELECT to_hex(CAST(v AS varbinary))
FROM memory.default.uint256_test
WHERE v > CAST(from_hex('01') AS uint256)
ORDER BY v;
```

### 4) 加法运算
```sql
-- 对列做 + 常量
SELECT id, to_hex(CAST(v + CAST(from_hex('01') AS uint256) AS varbinary)) AS v_plus_1
FROM memory.default.uint256_test
WHERE v IS NOT NULL
ORDER BY id;

-- 溢出将抛错
SELECT to_hex(CAST(CAST(from_hex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') AS uint256)
                   + CAST(from_hex('01') AS uint256) AS varbinary));
-- 报错：uint256 addition overflow
```

## 端到端测试思路（已在测试类中覆盖）
- 使用 `DistributedQueryRunner` + `MemoryPlugin` + `UInt256Plugin`
- 步骤：
  1. 创建表 `memory.default.uint256_test`
  2. 插入 `0x01`、`0x02`、`NULL`
  3. 比较与排序查询
  4. 加法（含进位）与溢出错误用例
  5. NULL 传播

对应实现见：`src/test/java/io/trino/plugin/uint256/TestUInt256Integration.java`

## 注意事项
- 规范化：通过 `CAST(... AS uint256)` 或 `uint256(...)` 写入时会左侧 0 填充至 32 字节（大端），保证比较/排序语义正确。
- 溢出：加法产生进位后仍非 0 的情况将抛出 `NUMERIC_VALUE_OUT_OF_RANGE`。
- 与十六进制文本互转：
  - 写入：`CAST(from_hex('...') AS uint256)` 或 `uint256(from_hex('...'))`
  - 读取：`to_hex(CAST(v AS varbinary))`
