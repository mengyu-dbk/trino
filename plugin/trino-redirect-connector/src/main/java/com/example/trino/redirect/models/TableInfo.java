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
package com.example.trino.redirect.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TableInfo
{
    private final long id;
    private final String name;
    private final int type;
    private final String location;

    @JsonCreator
    public TableInfo(
            @JsonProperty("id") long id,
            @JsonProperty("name") String name,
            @JsonProperty("type") int type,
            @JsonProperty("location") String location)
    {
        this.id = id;
        this.name = name;
        this.type = type;
        this.location = location;
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getType()
    {
        return type;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }
}
