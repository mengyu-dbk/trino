# Trino Redirect Connector

A Trino plugin that performs SQL rewriting through table redirection. This connector creates a virtual catalog that redirects queries to physical tables in other catalogs without storing any data itself.

## Architecture

The redirect connector is a **metadata-only layer** that intercepts table access and redirects queries to physical tables in other catalogs (like Hive, Iceberg, PostgreSQL, etc.).

### Key Features

- **Virtual Schema Boundary**: Redirection only applies to predefined virtual schemas (`virtual_sales`, `virtual_data`)
- **Table Redirection**: Transparently redirects virtual tables to physical tables in other catalogs
- **Loop Prevention**: Only redirects tables within configured virtual schemas
- **Simulated RPC**: Uses hardcoded mappings (can be replaced with real RPC/API calls)

### How It Works

When you query a table in the virtual catalog:

```sql
SELECT * FROM virtual.virtual_sales.daily_orders
```

The connector:
1. Checks if `virtual_sales` is a virtual schema (it is)
2. Looks up the physical table mapping for `virtual_sales.daily_orders`
3. Redirects the query to `hive.production.fact_orders_daily`
4. Returns results from the physical table

## Building the Plugin

From the Trino root directory:

```bash
./mvnw clean install -DskipTests -pl plugin/trino-redirect-connector
```

Or build the entire project:

```bash
./mvnw clean install -DskipTests
```

## Installation

1. Build the plugin (creates a zip file in `target/`)
2. Extract the plugin to Trino's plugin directory:
   ```bash
   unzip target/trino-redirect-connector-*.zip -d /path/to/trino/plugin/redirect
   ```

## Configuration

Create a catalog properties file at `etc/catalog/virtual.properties`:

```properties
connector.name=redirect
```

That's it! The connector requires no additional configuration properties in this simple implementation.

## Usage

### Show Available Schemas

```sql
SHOW SCHEMAS FROM virtual;
```

Output:
```
virtual_sales
virtual_data
```

### Query Virtual Tables

```sql
-- This redirects to hive.production.fact_orders_daily
SELECT * FROM virtual.virtual_sales.daily_orders;

-- This redirects to iceberg.analytics.dim_users
SELECT * FROM virtual.virtual_data.user_profiles;

-- This redirects to postgresql.public.products
SELECT * FROM virtual.virtual_data.product_catalog;
```

## Table Mappings

Current hardcoded mappings (simulating RPC responses):

| Virtual Table | Physical Table |
|--------------|----------------|
| `virtual_sales.daily_orders` | `hive.production.fact_orders_daily` |
| `virtual_sales.monthly_revenue` | `hive.production.fact_revenue_monthly` |
| `virtual_sales.customer_segments` | `iceberg.analytics.dim_customer_segments` |
| `virtual_data.user_profiles` | `iceberg.analytics.dim_users` |
| `virtual_data.activity_logs` | `hive.raw_data.fact_user_activity` |
| `virtual_data.product_catalog` | `postgresql.public.products` |

## Extending for Production

To use this in production with real RPC calls:

### 1. Add RPC Client Dependency

In `pom.xml`:
```xml
<dependency>
    <groupId>your.rpc.framework</groupId>
    <artifactId>rpc-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. Create RPC Client

```java
public interface TableMappingRpcClient {
    Optional<CatalogSchemaTableName> resolveTable(String schema, String table);
}
```

### 3. Update RedirectConnectorMetadata

Replace the hardcoded map in `redirectTable()`:

```java
// Old (simulated):
CatalogSchemaTableName physicalTable = TABLE_REDIRECTS.get(lookupKey);

// New (real RPC):
CatalogSchemaTableName physicalTable = rpcClient.resolveTable(schemaName, tableNameStr);
```

### 4. Configure RPC Endpoint

Add configuration properties in catalog file:

```properties
connector.name=redirect
redirect.rpc.endpoint=http://metadata-service:8080
redirect.rpc.timeout=5s
```

### 5. Read Configuration in Factory

Update `RedirectConnectorFactory`:

```java
@Override
public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
    String rpcEndpoint = config.get("redirect.rpc.endpoint");
    Duration timeout = Duration.valueOf(config.get("redirect.rpc.timeout"));

    TableMappingRpcClient rpcClient = new TableMappingRpcClient(rpcEndpoint, timeout);
    return new RedirectConnector(catalogName, rpcClient);
}
```

## Development

### Running Tests

```bash
./mvnw test -pl plugin/trino-redirect-connector
```

### Testing in Development Server

1. Add the plugin to `testing/trino-server-dev/etc/config.properties`:
   ```properties
   plugin.bundles=\
     ...,\
     ../../plugin/trino-redirect-connector/pom.xml
   ```

2. Create catalog config at `testing/trino-server-dev/etc/catalog/virtual.properties`:
   ```properties
   connector.name=redirect
   ```

3. Run `io.trino.server.DevelopmentServer`

4. Connect with CLI and test:
   ```sql
   SHOW SCHEMAS FROM virtual;
   SELECT * FROM virtual.virtual_sales.daily_orders;
   ```

## Key Implementation Details

### Loop Prevention

The connector prevents infinite redirection loops by checking if the schema is in the `VIRTUAL_SCHEMAS` set:

```java
if (!VIRTUAL_SCHEMAS.contains(schemaName)) {
    return Optional.empty();  // Don't redirect
}
```

This ensures:
- Queries to `hive.production.fact_orders_daily` are NOT redirected again
- Only tables in `virtual_sales` and `virtual_data` are redirected
- No infinite loops can occur

### Virtual Schema Discovery

The `listSchemaNames()` method returns only virtual schemas:

```java
@Override
public List<String> listSchemaNames(ConnectorSession session) {
    return ImmutableList.copyOf(VIRTUAL_SCHEMAS);
}
```

This makes `SHOW SCHEMAS FROM virtual;` work correctly.

### Table Discovery

The `listTables()` method returns an empty list because tables are discovered through redirection. You can enhance this to return virtual table names if desired.

## Architecture Diagram

```
┌─────────────────────────────────────────┐
│  Query: SELECT * FROM                   │
│  virtual.virtual_sales.daily_orders     │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  Redirect Connector                     │
│  - Check if schema in VIRTUAL_SCHEMAS   │
│  - Lookup physical table mapping        │
│  - Return redirection                   │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  Physical Query:                        │
│  SELECT * FROM                          │
│  hive.production.fact_orders_daily      │
└─────────────────────────────────────────┘
```

## Troubleshooting

### Plugin Not Loading

Check Trino server logs for errors:
```bash
tail -f /path/to/trino/var/log/server.log
```

Verify the plugin is in the correct directory:
```bash
ls /path/to/trino/plugin/redirect/
```

### Table Not Found

Verify the table mapping exists in `TABLE_REDIRECTS` map in `RedirectConnectorMetadata.java`.

### Infinite Redirection Loop

This should not happen if `VIRTUAL_SCHEMAS` is properly configured. Ensure physical catalog names are NOT in the `VIRTUAL_SCHEMAS` set.

## License

Apache License 2.0
