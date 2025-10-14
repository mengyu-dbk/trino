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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for the Redirect Connector.
 *
 * This class manages the configuration for the gRPC-based table mapping service.
 * Configuration properties are read from catalog properties files (e.g., redirect.properties).
 *
 * Example configuration:
 * <pre>
 * connector.name=redirect
 * redirect.grpc.host=localhost
 * redirect.grpc.port=50051
 * redirect.cache.ttl=10m
 * redirect.offline.catalog=iceberg
 * redirect.offline.schema=offline_data
 * redirect.online.catalog=memory
 * redirect.online.schema=default
 * </pre>
 */
public class RedirectConfig
{
    /**
     * gRPC service host
     */
    private String grpcHost = "localhost";

    /**
     * gRPC service port
     */
    private int grpcPort = 50051;

    /**
     * Cache TTL (Time To Live) for table mappings
     */
    private Duration cacheTtl = new Duration(10, TimeUnit.MINUTES);

    /**
     * Offline catalog name (for archived/historical data)
     */
    private String offlineCatalog = "iceberg";

    /**
     * Offline schema name
     */
    private String offlineSchema = "offline_data";

    /**
     * Online catalog name (for real-time data)
     */
    private String onlineCatalog = "memory";

    /**
     * Online schema name
     */
    private String onlineSchema = "default";

    @NotNull
    public String getGrpcHost()
    {
        return grpcHost;
    }

    @Config("redirect.grpc.host")
    @ConfigDescription("gRPC service host for table mapping")
    public RedirectConfig setGrpcHost(String grpcHost)
    {
        this.grpcHost = grpcHost;
        return this;
    }

    @NotNull
    public int getGrpcPort()
    {
        return grpcPort;
    }

    @Config("redirect.grpc.port")
    @ConfigDescription("gRPC service port for table mapping")
    public RedirectConfig setGrpcPort(int grpcPort)
    {
        this.grpcPort = grpcPort;
        return this;
    }

    @NotNull
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("redirect.cache.ttl")
    @ConfigDescription("Cache TTL for table mappings (e.g., 10m, 1h)")
    public RedirectConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    @NotNull
    public String getOfflineCatalog()
    {
        return offlineCatalog;
    }

    @Config("redirect.offline.catalog")
    @ConfigDescription("Offline catalog name for archived data")
    public RedirectConfig setOfflineCatalog(String offlineCatalog)
    {
        this.offlineCatalog = offlineCatalog;
        return this;
    }

    @NotNull
    public String getOfflineSchema()
    {
        return offlineSchema;
    }

    @Config("redirect.offline.schema")
    @ConfigDescription("Offline schema name")
    public RedirectConfig setOfflineSchema(String offlineSchema)
    {
        this.offlineSchema = offlineSchema;
        return this;
    }

    @NotNull
    public String getOnlineCatalog()
    {
        return onlineCatalog;
    }

    @Config("redirect.online.catalog")
    @ConfigDescription("Online catalog name for real-time data")
    public RedirectConfig setOnlineCatalog(String onlineCatalog)
    {
        this.onlineCatalog = onlineCatalog;
        return this;
    }

    @NotNull
    public String getOnlineSchema()
    {
        return onlineSchema;
    }

    @Config("redirect.online.schema")
    @ConfigDescription("Online schema name")
    public RedirectConfig setOnlineSchema(String onlineSchema)
    {
        this.onlineSchema = onlineSchema;
        return this;
    }
}
