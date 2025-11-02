# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Redirect Connector is a Trino plugin that implements **metadata-only table redirection**. It creates a virtual catalog that transparently redirects queries to physical tables in other catalogs (Hive, Iceberg, PostgreSQL, etc.) without storing any data itself.

Key concept: When you query `virtual.virtual_sales.daily_orders`, the connector redirects it to `hive.production.fact_orders_daily` or another physical table.

## Build Commands

### Build this plugin only
```bash
# From Trino root directory
./mvnw clean install -DskipTests -pl plugin/trino-redirect-connector

# Or with tests
./mvnw clean install -pl plugin/trino-redirect-connector
```

### Run tests
```bash
# All tests
./mvnw test -pl plugin/trino-redirect-connector

# Single test class
./mvnw test -pl plugin/trino-redirect-connector -Dtest=TestRedirectConnectorMetadata

# Single test method
./mvnw test -pl plugin/trino-redirect-connector -Dtest=TestRedirectConnectorSmokeTest#testSelectFromRedirectedTable
```

### Code quality
```bash
# From plugin directory or Trino root
./mvnw validate -pl plugin/trino-redirect-connector
./mvnw sortpom:sort -pl plugin/trino-redirect-connector
```

## Architecture

### Component Structure

1. **RedirectPlugin** (`RedirectPlugin.java:29-42`)
   - Entry point loaded by Trino's plugin framework
   - Registers the connector factory

2. **RedirectConnectorFactory** (`RedirectConnectorFactory.java`)
   - Creates connector instances
   - Handles configuration (currently minimal - only `connector.name=redirect`)

3. **RedirectConnector** (`RedirectConnector.java`)
   - Provides metadata instance to Trino
   - Lifecycle management

4. **RedirectConnectorMetadata** (`RedirectConnectorMetadata.java:42-225`)
   - **Core logic**: implements `redirectTable()` method
   - Defines virtual schemas (`VIRTUAL_SCHEMAS` set)
   - Maps virtual tables to physical tables (`TABLE_REDIRECTS` map)
   - Loop prevention: only redirects tables in virtual schemas

### Redirection Flow

```
Query: SELECT * FROM virtual.virtual_sales.daily_orders
           ↓
Trino Query Planner calls redirectTable()
           ↓
Check: Is "virtual_sales" in VIRTUAL_SCHEMAS? YES
           ↓
Lookup: TABLE_REDIRECTS.get("virtual_sales.daily_orders")
           ↓
Return: CatalogSchemaTableName("memory", "default", "orders")
           ↓
Trino rewrites query to: SELECT * FROM memory.default.orders
```

### Critical Implementation Details

**Loop Prevention** (`RedirectConnectorMetadata.java:127-130`):
- Only tables in `VIRTUAL_SCHEMAS` set are redirected
- Physical catalog names are NOT in this set
- This prevents infinite redirection loops

**Virtual Schema Boundary**:
- Virtual schemas: `virtual_sales`, `virtual_data`
- All other schemas return `Optional.empty()` from `redirectTable()`

**Table Mappings** (`RedirectConnectorMetadata.java:67-76`):
- Currently hardcoded in `TABLE_REDIRECTS` map
- In production, replace with RPC/API calls
- Format: `"schema.table"` → `CatalogSchemaTableName(catalog, schema, table)`

## Modifying Table Mappings

To add/change redirections, modify `TABLE_REDIRECTS` in `RedirectConnectorMetadata.java:67-76`:

```java
private static final Map<String, CatalogSchemaTableName> TABLE_REDIRECTS = ImmutableMap.<String, CatalogSchemaTableName>builder()
    .put("virtual_sales.daily_orders", new CatalogSchemaTableName("memory", "default", "orders"))
    .put("virtual_sales.new_table", new CatalogSchemaTableName("hive", "production", "new_table"))
    .buildOrThrow();
```

To add a new virtual schema, update `VIRTUAL_SCHEMAS` in `RedirectConnectorMetadata.java:52-54`:

```java
private static final Set<String> VIRTUAL_SCHEMAS = Set.of(
        "virtual_sales",
        "virtual_data",
        "virtual_analytics");
```

## Testing Infrastructure

### RedirectQueryRunner (`RedirectQueryRunner.java`)

Sets up a complete test environment with multiple catalogs:
- `virtual` - Redirect connector (under test)
- `memory` - Used as physical backend for redirected tables
- `tpch` - Provides test data (TPCH tiny dataset)

**Test data setup**:
- Creates 6 physical tables in memory connector
- Populates from TPCH data (15K orders, 1.5K customers, 200K parts)
- All table mappings redirect to memory connector for testing

### Test Classes

**Unit Tests**:
- `TestRedirectPlugin.java` - Plugin registration
- `TestRedirectConnectorFactory.java` - Factory creation and config
- `TestRedirectConnectorMetadata.java` - Metadata operations, loop prevention

**Integration Tests**:
- `TestRedirectConnectorSmokeTest.java` - Basic queries, JOINs, aggregations
- `TestRedirectTableRedirection.java` - Redirection logic, pushdown optimization
- `TestRedirectWithMultipleCatalogs.java` - Cross-catalog queries, complex JOINs

### Running Specific Test Scenarios

```bash
# Test all table mappings
./mvnw test -pl plugin/trino-redirect-connector -Dtest=TestRedirectConnectorMetadata#testAllTableMappings

# Test loop prevention
./mvnw test -pl plugin/trino-redirect-connector -Dtest=TestRedirectConnectorMetadata#testPreventInfiniteLoop

# Test cross-catalog JOIN
./mvnw test -pl plugin/trino-redirect-connector -Dtest=TestRedirectWithMultipleCatalogs#testJoinAcrossAllCatalogTypes
```

## Development Workflow

### Testing in Development Server

1. **Add plugin to dev server config** (`testing/trino-server-dev/etc/config.properties`):
   ```properties
   plugin.bundles=\
     ...,\
     ../../plugin/trino-redirect-connector/pom.xml
   ```

2. **Create catalog config** (`testing/trino-server-dev/etc/catalog/virtual.properties`):
   ```properties
   connector.name=redirect
   ```

3. **Run development server**:
   - Main class: `io.trino.server.DevelopmentServer`
   - Working directory: `testing/trino-server-dev`

4. **Test with CLI**:
   ```sql
   SHOW SCHEMAS FROM virtual;
   SELECT * FROM virtual.virtual_sales.daily_orders LIMIT 10;
   ```

### Debugging Redirection

Enable logging in `RedirectConnectorMetadata.java:123-149`:
- All redirection attempts are logged with `=== REDIRECT PLUGIN:` prefix
- Check logs to see which tables are being redirected
- Verify physical table resolution

Use EXPLAIN to see query rewriting:
```sql
EXPLAIN SELECT * FROM virtual.virtual_sales.daily_orders;
-- Output should show the physical table name
```

## Production Considerations

### Replacing Hardcoded Mappings with RPC

The current implementation uses a hardcoded map (`TABLE_REDIRECTS`). For production:

1. Create an RPC client interface
2. Add configuration properties for endpoint/timeout
3. Replace map lookup in `redirectTable()` with RPC call:
   ```java
   // Old: CatalogSchemaTableName physicalTable = TABLE_REDIRECTS.get(lookupKey);
   // New: CatalogSchemaTableName physicalTable = rpcClient.resolveTable(schemaName, tableNameStr);
   ```
4. Handle RPC failures gracefully (return `Optional.empty()` on timeout/error)

See README.md "Extending for Production" section for detailed implementation guide.

### Supported Operations

**Read operations** (fully supported):
- SELECT, JOIN, WHERE, GROUP BY, ORDER BY, LIMIT
- Subqueries, CTEs, UNION/INTERSECT/EXCEPT
- Window functions
- All optimization pushdowns (filter, projection, aggregation, limit)

**Write operations** (not supported):
- CREATE TABLE, INSERT, UPDATE, DELETE, DROP TABLE
- This is a metadata-only connector - writes go to physical catalogs directly

## Common Development Tasks

### Adding a new table mapping
1. Update `TABLE_REDIRECTS` in `RedirectConnectorMetadata.java:67-76`
2. Add corresponding test data in `RedirectQueryRunner.setupTestData()`
3. Add test case in `TestRedirectConnectorMetadata.testAllTableMappings()`
4. Run tests: `./mvnw test -pl plugin/trino-redirect-connector`

### Adding a new virtual schema
1. Update `VIRTUAL_SCHEMAS` in `RedirectConnectorMetadata.java:52-54`
2. Add table mappings for the new schema
3. Update tests to cover the new schema
4. Verify loop prevention still works

### Investigating test failures
```bash
# Run with debug logging
./mvnw test -pl plugin/trino-redirect-connector -X

# Check specific test output
./mvnw test -pl plugin/trino-redirect-connector -Dtest=FailingTest

# Increase heap if needed
export MAVEN_OPTS="-Xmx4g"
```
