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
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import static java.util.Objects.requireNonNull;

/**
 * Main connector implementation for the Redirect Connector.
 *
 * This connector is a metadata-only connector that doesn't handle data directly.
 * Instead, it redirects table references to physical tables in other catalogs.
 *
 * Key responsibilities:
 * - Provide connector metadata through RedirectConnectorMetadata
 * - Handle transaction lifecycle (begin/commit/rollback)
 * - Manage connector lifecycle (shutdown)
 */
public class RedirectConnector
        implements Connector
{
    private final String catalogName;
    private final RedirectConnectorMetadata metadata;

    /**
     * Creates a new RedirectConnector instance.
     *
     * @param catalogName The name of the catalog using this connector
     */
    public RedirectConnector(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metadata = new RedirectConnectorMetadata();
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
     * Override this method if you need to release resources (e.g., RPC connections,
     * thread pools, etc.)
     */
    @Override
    public void shutdown()
    {
        // No resources to release in this simple implementation
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
