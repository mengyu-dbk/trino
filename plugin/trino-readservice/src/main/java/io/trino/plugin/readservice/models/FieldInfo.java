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
package io.trino.plugin.readservice.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FieldInfo
{
    private final String name;
    private final String logicalType;
    private final String dataType;
    private final int length;
    private final boolean required;
    private final int order;

    @JsonCreator
    public FieldInfo(
            @JsonProperty("name") String name,
            @JsonProperty("logical_type") String logicalType,
            @JsonProperty("data_type") String dataType,
            @JsonProperty("length") int length,
            @JsonProperty("required") boolean required,
            @JsonProperty("order") int order)
    {
        this.name = name;
        this.logicalType = logicalType;
        this.dataType = dataType;
        this.length = length;
        this.required = required;
        this.order = order;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty("logical_type")
    public String getLogicalType()
    {
        return logicalType;
    }

    @JsonProperty("data_type")
    public String getDataType()
    {
        return dataType;
    }

    @JsonProperty
    public int getLength()
    {
        return length;
    }

    @JsonProperty
    public boolean isRequired()
    {
        return required;
    }

    @JsonProperty
    public int getOrder()
    {
        return order;
    }
}
