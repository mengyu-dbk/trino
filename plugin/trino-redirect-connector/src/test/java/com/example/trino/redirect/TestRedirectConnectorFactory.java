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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for RedirectConnectorFactory.
 *
 * Verifies connector creation and configuration handling.
 */
class TestRedirectConnectorFactory
{
    private final RedirectConnectorFactory factory = new RedirectConnectorFactory();

    @Test
    void testGetName()
    {
        assertThat(factory.getName())
                .as("Connector name should be 'redirect'")
                .isEqualTo("redirect");
    }

    @Test
    void testCreateConnector()
    {
        String catalogName = "virtual";
        Map<String, String> config = ImmutableMap.of();
        ConnectorContext context = new TestingConnectorContext();

        Connector connector = factory.create(catalogName, config, context);

        assertThat(connector)
                .as("Should create RedirectConnector instance")
                .isInstanceOf(RedirectConnector.class);
    }

    @Test
    void testCreateConnectorWithProperties()
    {
        // Verify connector can be created with properties (even if not used)
        String catalogName = "virtual";
        Map<String, String> config = ImmutableMap.of(
                "some.property", "value",
                "another.property", "123");
        ConnectorContext context = new TestingConnectorContext();

        Connector connector = factory.create(catalogName, config, context);

        assertThat(connector)
                .as("Should create connector even with unused properties")
                .isInstanceOf(RedirectConnector.class);
    }

    private static class TestingConnectorContext
            implements ConnectorContext
    {
        @Override
        public TypeManager getTypeManager()
        {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void testCreateConnectorRequiresNonNullCatalogName()
    {
        Map<String, String> config = ImmutableMap.of();
        ConnectorContext context = new TestingConnectorContext();

        assertThatThrownBy(() -> factory.create(null, config, context))
                .as("Should reject null catalog name")
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("catalogName is null");
    }

    @Test
    void testCreateConnectorRequiresNonNullConfig()
    {
        String catalogName = "virtual";
        ConnectorContext context = new TestingConnectorContext();

        assertThatThrownBy(() -> factory.create(catalogName, null, context))
                .as("Should reject null config")
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("config is null");
    }

    @Test
    void testCreateConnectorRequiresNonNullContext()
    {
        String catalogName = "virtual";
        Map<String, String> config = ImmutableMap.of();

        assertThatThrownBy(() -> factory.create(catalogName, config, null))
                .as("Should reject null context")
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("context is null");
    }
}
