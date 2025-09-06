# UINT256 类型支持

## 概述
提供在 Trino 中使用 256 位无符号整数（uint256）的类型与函数。

## 特性
- 类型名称: `UINT256`
- 底层表示: `VARBINARY()`（默认规范化为大端32字节，地址最高的字节存放最小的几位数）(如果有性能/存储放大问题，后面可以规划到使用char(32) 来实现）
- 支持能力:
  - 比较、排序、读写
  - 算术：加减乘除（`+`, `-`, `*`, `/`），溢出报错（NUMERIC_VALUE_OUT_OF_RANGE）
  - CAST：`varbinary ↔ uint256`, `varchar ↔ uint256`, `bigint -> uint256`
  - 便捷构造函数：`uint256(varbinary)`
- 空值支持: 与 SQL 一致
- 数值范围: 0 .. 2^256 - 1

说明：为保证排序/比较语义正确，推荐通过 `CAST(... AS uint256)` 或 `uint256(...)` 将值规范化为 32 字节大端表示；直接写入非 32 字节 varbinary 也能存储，但不保证排序一致性。

## 安装与注册
插件通过 `UInt256Plugin` 全局注册：
- `getTypes()` 注册类型 `uint256`
- `getFunctions()` 注册运算符与 CAST（包含加法、varbinary↔uint256、uint256(varbinary)）

## 使用方法

### 0) 如何测试
- 需要按照Trino根目录README的步骤编译，并确认能够启动开发服务器。
- Intellij 在项目结构-模块中导入pom.xml，加载所有模块。（有的时候开发一下就没法启动了，重新clone是最简单的修复方式）
- 启动 Trino 集群，加载 `trino-uint256` 插件（放置到 `plugin/` 目录下并重启）
- 使用 `memory` 连接器（或其他支持的连接器）
- 创建测试表，插入数据，执行查询。

### 1) 创建表（使用 memory connector）
```sql
create schema memory.default; -- using default memory catalog
use memory.default;
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

## 测试场景清单（当前代码已有覆盖）

### 单元测试（TestUInt256Query）
- 类型基本属性：displayName、可比/可排序、JavaType
- Block 读写：写入32字节、读取验证；appendTo 复制验证
- Null 处理：单值 null、混合多值含 null 的读取
- 多值写入：写入3个值逐一读取验证
- 类型签名：base/参数为空
- 边界值：全0、全FF（2^256-1）
- 可变长度写入：短于32字节、长于32字节的存取行为
- BlockBuilder 容量：按容量批量写入与读取
- bigint → uint256：正数、0、负数报错（函数与 CAST 运算符）
- varchar ↔ uint256（十进制）：
  - 无效空串报错
  - '15' 成功并回转为十进制字符串
  - 超范围十进制报错
  - 非法字符报错
- 算术：
  - 加法：0+0、1+1、0xFF+1 进位、大数相加
  - 加法溢出：max+1 报错
  - 多字节进位用例
  - 减法：0x0100-0x01 成功，下溢报错
  - 乘法：2*3 成功，上溢报错
  - 除法：0x10/0x04 成功，除0报错
- 位运算：AND、OR、XOR、NOT（与 to_hex 验证）

### 集成测试（TestUInt256Integration）
- DDL/插入/查询：
  - 建表 (UINT256 列)、插入 from_hex(...)→UINT256、含 NULL
  - 基本查询与 ORDER BY；NULL 计数
- 算术（SQL 层）：
  - 按列 + 常量（结果 to_hex 校验）
  - 溢出报错；NULL 传播
- BIGINT → UINT256（两种方式）：`CAST(bigint AS UINT256)` 与 `uint256(bigint)` 插入与查询
- 谓词/排序：`WHERE v > const` 且 `ORDER BY v`
- VARCHAR ↔ UINT256（十进制字符串）：
  - 正向：'1'、'255'、'15' → UINT256 并转 varbinary 验证
  - 反向：UINT256 → VARCHAR 十进制（如 from_hex('0A0B') → '2571'）
  - 错误：非法字符、超范围、负数、空字符串
- 其他算术：减法/乘法/除法（正常与错误场景）
- 位运算（SQL 函数）：bitwise_and / bitwise_or / bitwise_xor / bitwise_not（to_hex 验证）

## 覆盖进度（c完成/未完成/ur未二次确认）

- [c] 类型基本属性/类型签名
- [c] Block 读写、appendTo、容量、多值
- [x] 边界值（0、max=2^256-1）
- [x] CAST(varbinary ↔ uint256)
- [x] CAST(varchar ↔ uint256)（十进制）
- [x] CAST(bigint → uint256)
- [x] 算术（+、-、*、/）含溢出/下溢/除零错误
- [x] 位运算（AND/OR/XOR/NOT）
- [x] SQL 层：DDL、插入、查询、排序、谓词
- [x] NULL 传播（加法）
- [ ] NULL 传播覆盖更多运算（减/乘/除/位运算）
- [ ] VARCHAR 解析的健壮性：
  - 前后空白裁剪（如 ' 15 '）
  - 前导'+'号（如 '+15'）
  - 前导零（如 '00015' 的等价性明确验证）
- [ ] 十进制边界值：'0' 与 2^256-1 的十进制字符串正确性验证
- [ ] 更丰富的比较谓词：=、<、BETWEEN、IN 等
- [ ] 分组/聚合/去重：GROUP BY、ORDER BY 多键、DISTINCT
- [ ] 连接键：JOIN ON UINT256 的匹配/去重

> 说明：上述“未完成”项为建议补充的测试方向，不代表产品功能缺失；现有实现已支持相应能力（除明确未实现的聚合/函数外），建议通过新增测试用例增强回归覆盖度。

## 注意事项
- 规范化：通过 `CAST(... AS uint256)` 或 `uint256(...)` 写入时会左侧 0 填充至 32 字节（大端），保证比较/排序语义正确。
- 溢出：加法产生进位后仍非 0 的情况将抛出 `NUMERIC_VALUE_OUT_OF_RANGE`。
- 与十六进制文本互转：
  - 写入：`CAST(from_hex('...') AS uint256)` 或 `uint256(from_hex('...'))`
  - 读取：`to_hex(CAST(v AS varbinary))`
