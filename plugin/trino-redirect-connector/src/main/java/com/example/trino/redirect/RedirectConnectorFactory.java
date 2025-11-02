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

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating instances of the Redirect Connector.
 *
 * This factory is responsible for:
 * 1. Registering the connector with a unique name ("redirect")
 * 2. Creating new connector instances when a catalog using this connector is configured
 * 3. Passing configuration properties to the connector
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
     * Creates a new instance of the Redirect Connector.
     *
     * @param catalogName The name of the catalog being created (e.g., "virtual")
     * @param config Configuration properties from the catalog properties file
     * @param context The connector context provided by Trino
     * @return A new RedirectConnector instance
     */
    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        requireNonNull(context, "context is null");

        // Read MetaService endpoint from configuration
        String metaServiceEndpoint = config.get("metaservice.endpoint");

        // Create MetaServiceClient if endpoint is configured
        MetaServiceClient metaServiceClient = null;
        if (metaServiceEndpoint != null && !metaServiceEndpoint.isEmpty()) {
            metaServiceClient = new MetaServiceClient(metaServiceEndpoint);
        }

        return new RedirectConnector(catalogName, metaServiceClient);
    }
}
