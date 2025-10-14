# gRPC Integration Guide for Trino Redirect Connector

This document explains how the Redirect Connector has been enhanced with gRPC-based table mapping, inspired by chaintable-offline's architecture.

## Architecture Overview

The connector now uses the following components:

```
Query → RedirectConnectorMetadata
         ↓
       TableNameDecider (决策器)
         ↓
       TableMappingRpcClient (gRPC + Caffeine 缓存)
         ↓
       External gRPC Service (获取表元数据)
```

### Key Components

1. **RedirectConfig** - Configuration for gRPC endpoint and catalog mappings
2. **TableMappingService** - Interface for table metadata lookup
3. **TableMappingRpcClient** - gRPC client with Caffeine caching (10-minute TTL)
4. **TableNameDecider** - Logic to determine physical table location based on table type
5. **RedirectModule** - Guice dependency injection configuration

## Configuration

Create or update `testing/trino-server-dev/etc/catalog/redirect.properties`:

```properties
connector.name=redirect

# gRPC service endpoint
redirect.grpc.host=localhost
redirect.grpc.port=50051

# Cache TTL (duration format: e.g., 5m, 1h, 30s)
redirect.cache.ttl=10m

# Offline catalog configuration (for archived/historical data)
redirect.offline.catalog=iceberg
redirect.offline.schema=offline_data

# Online catalog configuration (for real-time data)
redirect.online.catalog=memory
redirect.online.schema=default
```

## Table Routing Logic

The connector determines physical table locations based on table metadata from the gRPC service:

### Table Types

1. **OFFCHAIN** → Always routes to **online catalog**
   - Uses `location` field from metadata
   - Example: `s3://bucket/data/my_table` → `memory.default.my_table`

2. **ONCHAIN_STATE** → Routes based on archive status
   - Normal: **online catalog** (uses `location`)
   - Archived (`.archived` suffix): **offline catalog** (uses `id_archived`)

3. **ONCHAIN_ITEM** → Always routes to **offline catalog**
   - Uses table `id` as physical table name
   - Example: table ID `12345` → `iceberg.offline_data.12345`

### Special Handling

- **Archived tables**: Append `.archived` to table name
  - `SELECT * FROM virtual.virtual_sales.my_table.archived`
  - Routes to offline storage with `{id}_archived` naming

## Integrating with Your gRPC Service

### Current Implementation

The current implementation uses **mock data** for demonstration. To integrate with a real gRPC service:

### Step 1: Define Your Proto File

Create `src/main/proto/table_mapping.proto`:

```protobuf
syntax = "proto3";

package tablemapping;

// Example proto - adapt to your service
service TableMappingService {
  rpc GetIdByName(GetIdByNameRequest) returns (GetIdByNameResponse);
  rpc GetTableMetadata(GetTableMetadataRequest) returns (GetTableMetadataResponse);
}

message GetIdByNameRequest {
  string name = 1;
}

message GetIdByNameResponse {
  int64 id = 1;
}

message GetTableMetadataRequest {
  int64 id = 1;
}

message GetTableMetadataResponse {
  Table table = 1;
}

message Table {
  int64 id = 1;
  string name = 2;
  TableType type = 3;
  string location = 4;
}

enum TableType {
  OFFCHAIN = 0;
  ONCHAIN_STATE = 1;
  ONCHAIN_ITEM = 2;
}
```

### Step 2: Update pom.xml

Uncomment the gRPC dependencies in `pom.xml`:

```xml
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty</artifactId>
    <version>1.60.0</version>
</dependency>

<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>1.60.0</version>
</dependency>

<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-stub</artifactId>
    <version>1.60.0</version>
</dependency>
```

Add protobuf-maven-plugin:

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.xolstice.maven.plugins</groupId>
            <artifactId>protobuf-maven-plugin</artifactId>
            <version>0.6.1</version>
            <configuration>
                <protocArtifact>com.google.protobuf:protoc:3.24.0:exe:${os.detected.classifier}</protocArtifact>
                <pluginId>grpc-java</pluginId>
                <pluginArtifact>io.grpc:protoc-gen-grpc-java:1.60.0:exe:${os.detected.classifier}</pluginArtifact>
            </configuration>
            <executions>
                <execution>
                    <goals>
                        <goal>compile</goal>
                        <goal>compile-custom</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

### Step 3: Update TableMappingRpcClient

Replace the mock implementation in `TableMappingRpcClient.java:getTableMetaRpc()` with real gRPC calls.

**See the commented code in `TableMappingRpcClient.java` for a complete example** (lines 125-175).

### Step 4: Build and Run

```bash
# Generate proto code
./mvnw clean compile

# Build the plugin
./mvnw clean install -DskipTests

# Run Trino server
# The connector will now use gRPC to fetch table mappings
```

## Using chaintable-offline Proto

If you want to use the exact same proto as chaintable-offline:

1. Copy the proto files from `chaintable-offline/trino-extends/src/main/proto/`
2. Add dependency on the generated classes
3. Update the imports in `TableMappingRpcClient.java`

Example:

```java
import metaservice.MetaServiceGrpc;
import metaservice.TableMeta.*;

// ... in constructor
ManagedChannel channel = ManagedChannelBuilder
    .forAddress(config.getGrpcHost(), config.getGrpcPort())
    .usePlaintext()
    .build();
this.metaStub = MetaServiceGrpc.newBlockingStub(channel);
```

## Testing

### 1. With Mock Implementation (Current)

```sql
-- These will use mock data
SELECT * FROM virtual.virtual_sales.eth_transfers;        -- ONCHAIN_ITEM → offline
SELECT * FROM virtual.virtual_sales.state_balances;       -- ONCHAIN_STATE → online
SELECT * FROM virtual.virtual_sales.user_profiles;        -- OFFCHAIN → online
SELECT * FROM virtual.virtual_sales.state_balances.archived;  -- → offline
```

### 2. With Real gRPC Service

Start your gRPC service on the configured host:port, then run queries:

```sql
SELECT * FROM virtual.virtual_sales.your_table_name;
```

Check logs for:
- gRPC connection status
- Cache hits/misses
- Table mapping decisions

## Performance Optimization

### Caching

- **Cache TTL**: Configured via `redirect.cache.ttl` (default: 10 minutes)
- **Cache Implementation**: Caffeine (high-performance)
- **Cache Key**: Table name
- **Cache Eviction**: Time-based (after write)

### Connection Pooling

gRPC automatically manages connection pooling via `ManagedChannel`.

### Monitoring

Add logging to track:
- Cache hit rate
- RPC latency
- Table mapping decisions

## Troubleshooting

### "Table not found" errors

- Check gRPC service is running and accessible
- Verify table exists in the gRPC service
- Check cache TTL if metadata changed recently

### Connection errors

- Verify `redirect.grpc.host` and `redirect.grpc.port`
- Check firewall rules
- Ensure gRPC service is using plaintext (or configure TLS)

### Wrong physical table

- Review `TableNameDecider` logic
- Check table type in gRPC response
- Verify catalog configurations (`redirect.offline.*`, `redirect.online.*`)

## Differences from Hardcoded Mappings

| Aspect | Before (Hardcoded) | After (gRPC) |
|--------|-------------------|-------------|
| **Table Mappings** | Static Map in code | Dynamic RPC calls |
| **Cache** | None | Caffeine (10min TTL) |
| **Configuration** | None | Properties file |
| **Extensibility** | Requires code changes | Configuration only |
| **Routing Logic** | Simple key-value | Type-based decisions |
| **DI Framework** | Manual instantiation | Guice injection |

## Next Steps

1. ✅ Implement your gRPC service (or use existing one)
2. ✅ Define proto file
3. ✅ Update `TableMappingRpcClient` with real RPC calls
4. ✅ Configure catalog properties
5. ✅ Build and test

## Reference Implementation

This implementation is inspired by:
- **chaintable-offline**: gRPC client architecture, caching strategy, table routing logic
- **Trino OPA plugin**: Guice dependency injection pattern
- **Trino HTTP Event Listener**: Airlift Bootstrap setup

For questions or issues, refer to the source code comments which contain detailed explanations.
