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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating instances of the Redirect Connector using Guice dependency injection.
 *
 * This factory is responsible for:
 * 1. Registering the connector with a unique name ("redirect")
 * 2. Initializing the Guice injector with all dependencies
 * 3. Creating new connector instances with injected dependencies
 * 4. Passing configuration properties to the connector
 *
 * The factory uses Airlift Bootstrap to:
 * - Load configuration from catalog properties files
 * - Initialize all components via Guice dependency injection
 * - Ensure proper lifecycle management
 *
 * Inspired by:
 * - trino-example-http/ExampleConnectorFactory
 * - trino-opa/OpaAccessControlFactory
 */
public class RedirectConnectorFactory
        implements ConnectorFactory
{
    /**
     * The unique name identifying this connector type.
     * This name is used in catalog configuration files (e.g., connector.name=redirect)
     */
    private static final String CONNECTOR_NAME = "redirect";

    /**
     * Returns the unique name of this connector.
     *
     * @return The connector name "redirect"
     */
    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    /**
     * Creates a new instance of the Redirect Connector using Guice dependency injection.
     *
     * This method:
     * 1. Initializes Airlift Bootstrap with JsonModule and RedirectModule
     * 2. Sets configuration properties from the catalog properties file
     * 3. Creates a Guice injector
     * 4. Retrieves the fully-injected RedirectConnector instance
     *
     * @param catalogName The name of the catalog being created (e.g., "virtual")
     * @param config Configuration properties from the catalog properties file
     * @param context The connector context provided by Trino
     * @return A new RedirectConnector instance with all dependencies injected
     */
    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        requireNonNull(context, "context is null");

        // Create Airlift Bootstrap with necessary modules
        Bootstrap app = new Bootstrap(
                new JsonModule(),      // JSON serialization support
                new RedirectModule()); // Redirect connector bindings

        // Initialize the injector with configuration
        Injector injector = app
                .doNotInitializeLogging()  // Trino handles logging
                .setRequiredConfigurationProperties(config)  // Load config from properties file
                .initialize();

        // Get the fully-injected connector instance from Guice
        return injector.getInstance(RedirectConnector.class);
    }
}
