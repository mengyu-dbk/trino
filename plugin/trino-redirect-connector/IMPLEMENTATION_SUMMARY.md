# Trino Redirect & ReadService Connectors 实现总结

## 项目概述

成功实现了两个Trino connector插件：
1. **trino-readservice**: 通过HTTP API执行SQL查询并返回结果的connector
2. **trino-redirect-connector**: 增强版redirect connector，集成MetaService进行智能表路由

## 已完成的组件

### 1. ReadService Connector (`plugin/trino-readservice/`)

#### 核心类
- **ReadServicePlugin.java**: 插件入口
- **ReadServiceConnectorFactory.java**: Connector工厂（使用Guice）
- **ReadServiceConnector.java**: Connector主类
- **ReadServiceModule.java**: Guice依赖注入模块
- **ReadServiceConfig.java**: 配置类
  - `readservice.endpoint` - ReadService API endpoint
  - `metaservice.endpoint` - MetaService API endpoint

#### HTTP客户端
- **ReadServiceClient.java**: HTTP客户端（参考example-http实现）
  - `executeQuery(String sql)` - POST查询到ReadService
  - `getTableIdByName(String tableName)` - 查询表ID
  - `getTableMetadata(long tableId)` - 查询表元信息
  - `getTableMetadataByName(String tableName)` - 便捷方法

#### 数据模型
- **QueryResult.java**: 查询结果（data + metadata）
- **QueryMetadata.java**: 查询metadata
- **ColumnInfo.java**: 列信息（name, type）
- **TableMetadata.java**: 表元信息
- **TableInfo.java**: 表基本信息
- **FieldInfo.java**: 字段信息

#### Metadata实现
- **ReadServiceMetadata.java**: 实现ConnectorMetadata接口
  - `listSchemaNames()` - 返回schema列表
  - `getTableHandle()` - 从MetaService获取表句柄
  - `getColumnHandles()` - 转换字段为列句柄
  - 类型映射：STRING→VARCHAR, INT→BIGINT, TIMESTAMP→TIMESTAMP(3)

#### 数据获取
- **ReadServiceSplit.java**: Split定义（包含schema和table信息）
- **ReadServiceSplitManager.java**: 创建单个split（整表查询）
- **ReadServiceRecordSetProvider.java**: 提供RecordSet
- **ReadServiceRecordSet.java**: RecordSet实现
- **ReadServiceRecordCursor.java**: 数据游标
  - 在构造时执行HTTP查询
  - 解析JSON结果为行数据
  - 实现数据访问方法（getLong, getSlice等）

### 2. Redirect Connector增强 (`plugin/trino-redirect-connector/`)

#### 新增组件
- **MetaServiceClient.java**: MetaService HTTP客户端
  - `getTableIdByName(String tableName)` - 查询表ID
  - `getTableMetadata(long tableId)` - 查询表元信息
  - `getTableMetadataByName(String tableName)` - 便捷方法

#### 数据模型
- **models/TableMetadata.java**: 表元信息
- **models/TableInfo.java**: 表基本信息（id, name, type, location）

#### 修改的组件
- **RedirectConnectorFactory.java**:
  - 读取`metaservice.endpoint`配置
  - 创建并注入MetaServiceClient

- **RedirectConnector.java**:
  - 接受MetaServiceClient参数
  - 传递给RedirectConnectorMetadata

- **RedirectConnectorMetadata.java**:
  - 集成MetaServiceClient
  - 更新`redirectTable()`方法实现智能路由：
    1. 检查是否在虚拟schema中
    2. 如果MetaServiceClient配置，查询表元信息
    3. 所有表重定向到`readservice` catalog
    4. 使用表ID作为物理表名
    5. 失败时回退到静态映射

## 配置示例

### ReadService Connector
**文件**: `etc/catalog/readservice.properties`
```properties
connector.name=readservice
readservice.endpoint=http://your-readservice-api:8080
metaservice.endpoint=http://your-metaservice-api:8080
```

### Redirect Connector（增强版）
**文件**: `etc/catalog/virtual.properties`
```properties
connector.name=redirect
metaservice.endpoint=http://your-metaservice-api:8080
```

## 使用流程

### 1. 编译插件
```bash
# 从Trino根目录
./mvnw clean install -DskipTests -pl plugin/trino-readservice
./mvnw clean install -DskipTests -pl plugin/trino-redirect-connector
```

### 2. 配置catalogs
创建上述配置文件到`etc/catalog/`目录

### 3. 查询流程
```sql
-- 用户查询虚拟表
SELECT * FROM virtual.virtual_sales.daily_orders LIMIT 10;

-- Redirect connector处理：
-- 1. 检测到virtual_sales是虚拟schema
-- 2. 调用MetaService获取daily_orders的表信息（假设tableId=9247）
-- 3. 重定向到：readservice.virtual_sales.9247

-- ReadService connector处理：
-- 1. 接收到查询readservice.virtual_sales.9247
-- 2. 构建SQL: SELECT * FROM virtual_sales.9247 LIMIT 10
-- 3. 通过HTTP POST到ReadService API执行查询
-- 4. 解析JSON响应并返回结果给Trino
```

## 架构图

```
┌─────────────────────────────────────────────┐
│ User Query:                                 │
│ SELECT * FROM virtual.virtual_sales.orders  │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│ Redirect Connector                          │
│ ├─ Check: Is "virtual_sales" virtual?  ✓   │
│ ├─ Call MetaService API                     │
│ │  GET /api/v1/meta/get_id_by_name?name=... │
│ │  GET /api/v1/meta/get_table_metadata?id=..│
│ └─ Redirect to: readservice.virtual_sales.ID│
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│ ReadService Connector                       │
│ ├─ Build SQL: SELECT * FROM schema.tableId │
│ ├─ Call ReadService API                     │
│ │  POST /api/v1/table/query                 │
│ │  Body: {"sql": "SELECT ..."}              │
│ ├─ Parse JSON response                      │
│ └─ Return data to Trino                     │
└─────────────────────────────────────────────┘
```

## 特性

### ReadService Connector
✅ 完整的HTTP客户端实现（参考example-http模式）
✅ MetaService集成获取表结构
✅ JSON响应解析
✅ 类型转换（STRING/INT/TIMESTAMP → Trino类型）
✅ RecordCursor实现支持数据读取
✅ 简化实现，无缓存

### Redirect Connector
✅ MetaService集成
✅ 智能表路由（基于表元信息）
✅ 回退机制（MetaService失败时使用静态映射）
✅ 循环防护（只重定向虚拟schema）
✅ 详细日志记录

## 简化设计说明

根据需求，本实现采用了简化设计：
- **无缓存**: 不使用Caffeine等缓存中间件
- **简化路由**: 所有表统一重定向到readservice，未实现复杂的online/offline路由逻辑
- **直接HTTP**: 使用java.net.http.HttpClient，未使用其他HTTP客户端库
- **无Calcite集成**: 未嵌入SQL rewriter，简单转发查询

## 测试建议

### 1. 单元测试
创建基础测试验证组件功能：
```bash
./mvnw test -pl plugin/trino-readservice -Dtest=TestReadServiceClient
./mvnw test -pl plugin/trino-redirect-connector -Dtest=TestRedirectWithMetaService
```

### 2. 集成测试
启动Trino dev server进行端到端测试：
```bash
# 在IntelliJ中运行 io.trino.server.DevelopmentServer
# 然后使用CLI测试
./client/trino-cli/target/trino-cli-*-executable.jar

trino> SHOW SCHEMAS FROM virtual;
trino> SELECT * FROM virtual.virtual_sales.daily_orders LIMIT 10;
```

### 3. Mock测试
使用WireMock模拟MetaService和ReadService API进行测试

## 下一步工作

### 必要的完善
1. **添加单元测试**: 测试各个组件的功能
2. **错误处理增强**: 更细致的异常处理和重试机制
3. **参数验证**: 验证配置参数的有效性

### 可选的增强
1. **缓存支持**: 添加Caffeine缓存MetaService响应
2. **复杂路由**: 实现基于表类型的online/offline路由决策
3. **连接池**: 优化HTTP客户端连接管理
4. **监控指标**: 添加查询统计和性能监控

## 文件清单

### ReadService Connector (新建)
```
plugin/trino-readservice/
├── pom.xml
└── src/main/java/io/trino/plugin/readservice/
    ├── ReadServicePlugin.java
    ├── ReadServiceConnectorFactory.java
    ├── ReadServiceConnector.java
    ├── ReadServiceModule.java
    ├── ReadServiceTransactionHandle.java
    ├── ReadServiceConfig.java
    ├── ReadServiceClient.java
    ├── ReadServiceMetadata.java
    ├── ReadServiceTableHandle.java
    ├── ReadServiceColumnHandle.java
    ├── ReadServiceSplit.java
    ├── ReadServiceSplitManager.java
    ├── ReadServiceRecordSetProvider.java
    ├── ReadServiceRecordSet.java
    ├── ReadServiceRecordCursor.java
    └── models/
        ├── QueryResult.java
        ├── QueryMetadata.java
        ├── ColumnInfo.java
        ├── TableMetadata.java
        ├── TableInfo.java
        └── FieldInfo.java
```

### Redirect Connector (修改)
```
plugin/trino-redirect-connector/
├── pom.xml (已更新：添加Jackson依赖)
└── src/main/java/com/example/trino/redirect/
    ├── RedirectConnectorFactory.java (已修改：读取配置，创建MetaServiceClient)
    ├── RedirectConnector.java (已修改：接受MetaServiceClient参数)
    ├── RedirectConnectorMetadata.java (已修改：集成MetaService路由)
    ├── MetaServiceClient.java (新建)
    └── models/
        ├── TableMetadata.java (新建)
        └── TableInfo.java (新建)
```

## 总结

成功实现了简化版的ReadService connector和增强版的Redirect connector，两者通过MetaService API进行集成。核心功能已完整实现，可以进行端到端测试。实现参考了trino-example-http的最佳实践，代码结构清晰，易于维护和扩展。
