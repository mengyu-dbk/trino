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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

/**
 * Main plugin entry point for the Redirect Connector.
 *
 * This plugin creates a virtual catalog that redirects table access to physical tables
 * in other catalogs without storing any data itself. It acts as a metadata-only layer
 * for table redirection.
 *
 * The plugin is loaded by Trino's plugin framework through the service loader mechanism.
 */
public class RedirectPlugin
        implements Plugin
{
    /**
     * Returns the list of connector factories provided by this plugin.
     *
     * @return A list containing the RedirectConnectorFactory
     */
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RedirectConnectorFactory());
    }
}
