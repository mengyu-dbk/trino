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

import static java.util.Objects.requireNonNull;

/**
 * Metadata for a table retrieved from the external RPC service.
 *
 * This class represents the table metadata returned by the table mapping service.
 * It contains information needed to determine the physical location of a table.
 *
 * Inspired by chaintable-offline's Table proto definition.
 */
public class TableMetadata
{
    /**
     * Table types that determine where the data is stored
     */
    public enum TableType
    {
        /**
         * Off-chain data stored in offline storage (e.g., Iceberg, Hive)
         */
        OFFCHAIN,

        /**
         * On-chain state data, may be in online or offline storage depending on archive status
         */
        ONCHAIN_STATE,

        /**
         * On-chain item/event data, always in offline storage
         */
        ONCHAIN_ITEM
    }

    private final long id;
    private final String name;
    private final TableType type;
    private final String location;

    public TableMetadata(long id, String name, TableType type, String location)
    {
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.location = location; // may be null
    }

    public long getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public TableType getType()
    {
        return type;
    }

    public String getLocation()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return "TableMetadata{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", location='" + location + '\'' +
                '}';
    }
}
