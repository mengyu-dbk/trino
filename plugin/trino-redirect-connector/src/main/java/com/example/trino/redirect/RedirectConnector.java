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

import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import static java.util.Objects.requireNonNull;

/**
 * Main connector implementation for the Redirect Connector with Guice dependency injection.
 *
 * This connector is a metadata-only connector that doesn't handle data directly.
 * Instead, it redirects table references to physical tables in other catalogs using
 * gRPC-based table mapping service.
 *
 * Key responsibilities:
 * - Provide connector metadata through RedirectConnectorMetadata
 * - Handle transaction lifecycle (begin/commit/rollback)
 * - Manage connector lifecycle (shutdown)
 * - Clean up resources (RPC connections, caches) on shutdown
 */
public class RedirectConnector
        implements Connector
{
    private final RedirectConnectorMetadata metadata;
    private final TableMappingService tableMappingService;

    /**
     * Creates a new RedirectConnector instance with Guice-injected dependencies.
     *
     * @param metadata The connector metadata implementation
     * @param tableMappingService The table mapping service for resource cleanup
     */
    @Inject
    public RedirectConnector(
            RedirectConnectorMetadata metadata,
            TableMappingService tableMappingService)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.tableMappingService = requireNonNull(tableMappingService, "tableMappingService is null");
    }

    /**
     * Begins a new transaction.
     *
     * Since this is a read-only metadata connector with no state, we use a simple
     * transaction handle. In a real implementation with stateful operations,
     * you would track transaction state here.
     *
     * @param isolationLevel The transaction isolation level
     * @param readOnly Whether this is a read-only transaction
     * @param autoCommit Whether the transaction should auto-commit
     * @return A transaction handle
     */
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        // Return a simple transaction handle since we have no transactional state
        return RedirectTransactionHandle.INSTANCE;
    }

    /**
     * Returns the connector metadata implementation.
     *
     * @param session The connector session
     * @param transactionHandle The transaction handle
     * @return The RedirectConnectorMetadata instance
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    /**
     * Commits a transaction.
     *
     * Since this connector is read-only and stateless, this is a no-op.
     *
     * @param transactionHandle The transaction handle to commit
     */
    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        // No-op: no transactional state to commit
    }

    /**
     * Rolls back a transaction.
     *
     * Since this connector is read-only and stateless, this is a no-op.
     *
     * @param transactionHandle The transaction handle to rollback
     */
    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        // No-op: no transactional state to rollback
    }

    /**
     * Performs cleanup when the connector is shut down.
     *
     * This method releases resources including:
     * - gRPC channels
     * - Cache resources
     * - Connection pools
     */
    @Override
    public void shutdown()
    {
        // Shutdown the table mapping service to release gRPC connections and caches
        tableMappingService.shutdown();
    }

    /**
     * Simple transaction handle for the redirect connector.
     *
     * Since we have no transactional state, we use a singleton instance.
     */
    private enum RedirectTransactionHandle
            implements ConnectorTransactionHandle
    {
        INSTANCE
    }
}
