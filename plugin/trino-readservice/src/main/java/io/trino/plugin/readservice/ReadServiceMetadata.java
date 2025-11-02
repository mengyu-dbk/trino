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
package io.trino.plugin.readservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.readservice.models.FieldInfo;
import io.trino.plugin.readservice.models.TableMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class ReadServiceMetadata
        implements ConnectorMetadata
{
    private final ReadServiceClient client;

    @Inject
    public ReadServiceMetadata(ReadServiceClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        // Simplified: return a default schema
        // In a real implementation, this would query MetaService for available schemas
        return ImmutableList.of("default");
    }

    @Override
    public ReadServiceTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        // Verify schema exists
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        // Try to get table metadata from MetaService
        try {
            String fullTableName = tableName.getSchemaName() + "." + tableName.getTableName();
            TableMetadata metadata = client.getTableMetadataByName(fullTableName);
            if (metadata == null || metadata.getTable() == null) {
                return null;
            }
            return new ReadServiceTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }
        catch (Exception e) {
            // Table not found or error accessing MetaService
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ReadServiceTableHandle handle = (ReadServiceTableHandle) tableHandle;
        return getTableMetadata(handle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        // Simplified: return empty list
        // In a real implementation, this would query MetaService for table list
        return ImmutableList.of();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ReadServiceTableHandle readServiceTableHandle = (ReadServiceTableHandle) tableHandle;

        String fullTableName = readServiceTableHandle.getSchemaName() + "." + readServiceTableHandle.getTableName();
        TableMetadata metadata = client.getTableMetadataByName(fullTableName);

        if (metadata == null || metadata.getFields() == null) {
            throw new TableNotFoundException(readServiceTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (FieldInfo field : metadata.getFields()) {
            Type trinoType = convertToTrinoType(field.getLogicalType());
            columnHandles.put(
                    field.getName(),
                    new ReadServiceColumnHandle(field.getName(), trinoType, index));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((ReadServiceColumnHandle) columnHandle).getColumnMetadata();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames(null).contains(tableName.getSchemaName())) {
            return null;
        }

        try {
            String fullTableName = tableName.getSchemaName() + "." + tableName.getTableName();
            TableMetadata metadata = client.getTableMetadataByName(fullTableName);

            if (metadata == null || metadata.getFields() == null) {
                return null;
            }

            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (FieldInfo field : metadata.getFields()) {
                Type trinoType = convertToTrinoType(field.getLogicalType());
                columns.add(new ColumnMetadata(field.getName(), trinoType));
            }

            return new ConnectorTableMetadata(tableName, columns.build());
        }
        catch (Exception e) {
            return null;
        }
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    /**
     * Convert ReadService logical type to Trino type
     *
     * @param logicalType Logical type from ReadService (STRING, INT, TIMESTAMP, etc.)
     * @return Corresponding Trino Type
     */
    private Type convertToTrinoType(String logicalType)
    {
        return switch (logicalType.toUpperCase()) {
            case "STRING", "BLOCKID" -> VarcharType.VARCHAR;
            case "INT", "LONG", "BIGINT" -> BigintType.BIGINT;
            case "TIMESTAMP" -> TimestampType.TIMESTAMP_MILLIS;
            default -> VarcharType.VARCHAR; // Default fallback
        };
    }
}
