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
package com.example.trino.redirect;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

/**
 * gRPC-based implementation of TableMappingService with Caffeine caching.
 *
 * This class is inspired by chaintable-offline's MetaServiceImpl and provides:
 * - gRPC client for table metadata lookup
 * - Caffeine cache for performance optimization
 * - Automatic cache expiration
 *
 * IMPORTANT: This is a template implementation. To use with your own gRPC service:
 *
 * 1. Add your proto definition and generate Java code:
 *    - Place your .proto file in src/main/proto/
 *    - Add protobuf-maven-plugin to pom.xml
 *    - Run: mvn clean compile
 *
 * 2. Replace the mock implementation in getTableMetaRpc() with real gRPC calls:
 *    <pre>
 *    // Example using chaintable-offline proto:
 *    import metaservice.MetaServiceGrpc;
 *    import metaservice.TableMeta.*;
 *
 *    private final MetaServiceBlockingStub metaStub;
 *
 *    // In constructor:
 *    ManagedChannel channel = ManagedChannelBuilder
 *        .forAddress(config.getGrpcHost(), config.getGrpcPort())
 *        .usePlaintext()
 *        .build();
 *    this.metaStub = MetaServiceGrpc.newBlockingStub(channel);
 *
 *    // In getTableMetaRpc():
 *    GetIdByNameRequest req = GetIdByNameRequest.newBuilder()
 *        .setName(tableName)
 *        .build();
 *    GetIdByNameResponse resp = metaStub.getIdByName(req);
 *
 *    GetTableMetadataRequest reqMeta = GetTableMetadataRequest.newBuilder()
 *        .setId(resp.getId())
 *        .build();
 *    GetTableMetadataResponse respMeta = metaStub.getTableMetadata(reqMeta);
 *    Table protoTable = respMeta.getTableMetadata().getTable();
 *
 *    // Convert proto Table to TableMetadata
 *    return convertProtoToTableMetadata(protoTable);
 *    </pre>
 */
public class TableMappingRpcClient
        implements TableMappingService
{
    private static final Logger log = Logger.get(TableMappingRpcClient.class);

    private final Cache<String, TableMetadata> cache;
    private final RedirectConfig config;

    // TODO: Add gRPC channel and stub when integrating with real gRPC service
    // private final ManagedChannel channel;
    // private final YourServiceBlockingStub stub;

    @Inject
    public TableMappingRpcClient(RedirectConfig config)
    {
        this.config = config;

        // Initialize Caffeine cache with configured TTL
        Duration cacheTtl = config.getCacheTtl();
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(cacheTtl.toJavaTime())
                .build();

        log.info("TableMappingRpcClient initialized with gRPC endpoint %s:%d, cache TTL: %s",
                config.getGrpcHost(), config.getGrpcPort(), cacheTtl);

        // TODO: Initialize gRPC channel when integrating with real service
        // this.channel = ManagedChannelBuilder
        //     .forAddress(config.getGrpcHost(), config.getGrpcPort())
        //     .usePlaintext()  // or .useTransportSecurity() for TLS
        //     .build();
        // this.stub = YourServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public TableMetadata getTableMetaByName(String tableName)
            throws TableMappingException
    {
        log.debug("Looking up table metadata for: %s", tableName);

        // Check cache first
        TableMetadata cached = cache.getIfPresent(tableName);
        if (cached != null) {
            log.debug("Cache hit for table: %s", tableName);
            return cached;
        }

        // Cache miss - fetch from RPC service
        log.debug("Cache miss for table: %s, fetching from RPC service", tableName);
        TableMetadata metadata = getTableMetaRpc(tableName);

        // Store in cache
        cache.put(tableName, metadata);

        return metadata;
    }

    /**
     * Performs the actual RPC call to retrieve table metadata.
     *
     * CURRENT IMPLEMENTATION: Mock data for demonstration
     * PRODUCTION: Replace with real gRPC calls (see class-level comments)
     */
    private TableMetadata getTableMetaRpc(String tableName)
            throws TableMappingException
    {
        log.info("Fetching table metadata from RPC service for: %s", tableName);

        // =================================================================
        // MOCK IMPLEMENTATION - Replace with real gRPC calls
        // =================================================================
        // This mock implementation returns fake data for demonstration.
        // In production, replace this with actual gRPC calls to your service.

        try {
            // Simulate different table types based on naming convention
            if (tableName.startsWith("eth_")) {
                // Simulate ONCHAIN_ITEM type
                return new TableMetadata(
                        generateMockId(tableName),
                        tableName,
                        TableMetadata.TableType.ONCHAIN_ITEM,
                        null);
            }
            else if (tableName.startsWith("state_")) {
                // Simulate ONCHAIN_STATE type
                return new TableMetadata(
                        generateMockId(tableName),
                        tableName,
                        TableMetadata.TableType.ONCHAIN_STATE,
                        "s3://bucket/state/" + tableName);
            }
            else {
                // Simulate OFFCHAIN type
                return new TableMetadata(
                        generateMockId(tableName),
                        tableName,
                        TableMetadata.TableType.OFFCHAIN,
                        "s3://bucket/offchain/" + tableName);
            }
        }
        catch (Exception e) {
            log.error(e, "RPC call failed for table: %s", tableName);
            throw new TableMappingException("Failed to fetch table metadata for: " + tableName, e);
        }

        // =================================================================
        // REAL gRPC IMPLEMENTATION EXAMPLE (commented out):
        // =================================================================
        /*
        try {
            // Step 1: Get table ID by name
            GetIdByNameRequest idRequest = GetIdByNameRequest.newBuilder()
                    .setName(tableName)
                    .build();
            GetIdByNameResponse idResponse = stub.getIdByName(idRequest);

            // Step 2: Get table metadata by ID
            GetTableMetadataRequest metaRequest = GetTableMetadataRequest.newBuilder()
                    .setId(idResponse.getId())
                    .build();
            GetTableMetadataResponse metaResponse = stub.getTableMetadata(metaRequest);

            // Step 3: Extract and validate table from response
            Table protoTable = metaResponse.getTableMetadata().getTable();
            if (protoTable.equals(Table.getDefaultInstance())) {
                throw new TableMappingException("Table not found: " + tableName);
            }

            // Step 4: Convert proto Table to TableMetadata
            TableMetadata.TableType type;
            switch (protoTable.getType()) {
                case OFFCHAIN:
                    type = TableMetadata.TableType.OFFCHAIN;
                    break;
                case ONCHAIN_STATE:
                    type = TableMetadata.TableType.ONCHAIN_STATE;
                    break;
                case ONCHAIN_ITEM:
                    type = TableMetadata.TableType.ONCHAIN_ITEM;
                    break;
                default:
                    throw new TableMappingException("Unknown table type: " + protoTable.getType());
            }

            return new TableMetadata(
                    protoTable.getId(),
                    protoTable.getName(),
                    type,
                    protoTable.getLocation());
        }
        catch (StatusRuntimeException e) {
            log.error(e, "gRPC call failed for table: %s", tableName);
            throw new TableMappingException("RPC call failed for table: " + tableName, e);
        }
        */
    }

    /**
     * Generates a mock table ID based on the table name.
     * Remove this when using real gRPC.
     */
    private long generateMockId(String tableName)
    {
        return Math.abs(tableName.hashCode());
    }

    @Override
    public void shutdown()
    {
        log.info("Shutting down TableMappingRpcClient");
        cache.invalidateAll();

        // TODO: Shutdown gRPC channel when using real implementation
        // if (channel != null && !channel.isShutdown()) {
        //     try {
        //         channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        //     }
        //     catch (InterruptedException e) {
        //         Thread.currentThread().interrupt();
        //         channel.shutdownNow();
        //     }
        // }
    }
}
