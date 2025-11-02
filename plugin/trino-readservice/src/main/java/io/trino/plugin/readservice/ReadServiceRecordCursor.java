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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.readservice.models.QueryResult;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ReadServiceRecordCursor
        implements RecordCursor
{
    private final List<ReadServiceColumnHandle> columnHandles;
    private final Iterator<Map<String, Object>> rowIterator;
    private Map<String, Object> currentRow;
    private final long totalBytes;

    public ReadServiceRecordCursor(
            ReadServiceClient client,
            ReadServiceSplit split,
            List<ReadServiceColumnHandle> columnHandles)
    {
        requireNonNull(client, "client is null");
        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        // Execute query via ReadService API
        // Build SQL query: SELECT * FROM schema.table
        String sql = String.format("SELECT * FROM %s.%s",
                split.getSchemaName(),
                split.getTableName());

        QueryResult queryResult = client.executeQuery(sql);

        // Get row data
        List<Map<String, Object>> rows = queryResult.getData();
        this.rowIterator = rows.iterator();

        // Estimate total bytes (rough estimate based on JSON size)
        this.totalBytes = rows.size() * 100L; // Rough estimate
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!rowIterator.hasNext()) {
            currentRow = null;
            return false;
        }
        currentRow = rowIterator.next();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        Object value = getFieldValue(field);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public long getLong(int field)
    {
        Object value = getFieldValue(field);
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public double getDouble(int field)
    {
        Object value = getFieldValue(field);
        if (value == null) {
            return 0.0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public Slice getSlice(int field)
    {
        Object value = getFieldValue(field);
        if (value == null) {
            return Slices.EMPTY_SLICE;
        }
        return Slices.utf8Slice(value.toString());
    }

    @Override
    public Object getObject(int field)
    {
        return getFieldValue(field);
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        Object value = getFieldValue(field);
        return value == null;
    }

    @Override
    public void close()
    {
        // No resources to close
    }

    private Object getFieldValue(int field)
    {
        checkState(currentRow != null, "Cursor has not been advanced yet");
        checkArgument(field < columnHandles.size(), "Invalid field index");

        String columnName = columnHandles.get(field).getColumnName();
        return currentRow.get(columnName);
    }
}
