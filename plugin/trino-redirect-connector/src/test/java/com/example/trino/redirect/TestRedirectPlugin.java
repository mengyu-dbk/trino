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

import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for RedirectPlugin.
 *
 * Verifies that the plugin correctly registers the connector factory.
 */
class TestRedirectPlugin
{
    @Test
    void testGetConnectorFactories()
    {
        RedirectPlugin plugin = new RedirectPlugin();
        Iterable<ConnectorFactory> factories = plugin.getConnectorFactories();

        assertThat(factories)
                .as("Plugin should provide exactly one connector factory")
                .hasSize(1);

        ConnectorFactory factory = factories.iterator().next();

        assertThat(factory)
                .as("Factory should be RedirectConnectorFactory")
                .isInstanceOf(RedirectConnectorFactory.class);

        assertThat(factory.getName())
                .as("Factory name should be 'redirect'")
                .isEqualTo("redirect");
    }
}
