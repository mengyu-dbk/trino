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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for the Redirect Connector.
 *
 * This module configures all dependency injection bindings for the connector:
 * - Configuration (RedirectConfig)
 * - RPC client (TableMappingService → TableMappingRpcClient)
 * - Table name decider (TableNameDecider)
 * - Connector metadata (RedirectConnectorMetadata)
 * - Connector (RedirectConnector)
 *
 * Inspired by Trino's standard connector modules:
 * - trino-example-http/ExampleModule
 * - trino-opa/OpaAccessControlModule
 */
public class RedirectModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // Bind configuration class
        // This reads properties from the catalog properties file
        configBinder(binder).bindConfig(RedirectConfig.class);

        // Bind TableMappingService to its gRPC implementation
        // SINGLETON scope ensures only one instance is created and shared
        binder.bind(TableMappingService.class)
                .to(TableMappingRpcClient.class)
                .in(Scopes.SINGLETON);

        // Bind TableNameDecider
        binder.bind(TableNameDecider.class)
                .in(Scopes.SINGLETON);

        // Bind ConnectorMetadata
        binder.bind(RedirectConnectorMetadata.class)
                .in(Scopes.SINGLETON);

        // Bind Connector
        binder.bind(RedirectConnector.class)
                .in(Scopes.SINGLETON);
    }
}
