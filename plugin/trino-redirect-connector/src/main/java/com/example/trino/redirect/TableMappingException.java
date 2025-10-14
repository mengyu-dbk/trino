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

/**
 * Exception thrown when table mapping operations fail.
 *
 * This can occur when:
 * - The table is not found in the mapping service
 * - The RPC call to the mapping service fails
 * - Network issues or service unavailability
 */
public class TableMappingException
        extends Exception
{
    public TableMappingException(String message)
    {
        super(message);
    }

    public TableMappingException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
