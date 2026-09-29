/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.service.cql.api.lookup;

import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.CQLQueryCallback;

/**
 * One cell of a result row: a column name and its value, with no schema imposed. Where
 * {@link CQLQueryCallback} delivers rows as typed NiFi records, a cell hands back what the server sent -
 * for a caller that owns its own encoding (opaque bytes, say) and would only have to undo any conversion.
 */
public interface CqlCell {

    /**
     * The column name, exactly as the server sent it. Not necessarily a legal CQL identifier
     * ({@code writetime(v)}, {@code [applied]}), nor unique within a row ({@code SELECT v, writetime(v), v})
     * - which is why {@link CqlRow} is a list rather than a map.
     */
    String columnName();

    /**
     * Whether this cell holds a CQL null - distinct from the column being absent from the row, which is
     * {@link CqlRow#findCell(String)}.
     */
    boolean isNull();

    /**
     * The value as binary (a fresh copy the caller may keep and modify), or {@code null} if the cell is
     * null. The accessor for opaque bytes, and the one an implementation must normalize across backends.
     *
     * @throws UnsupportedOperationException if the column's type is not binary
     */
    byte[] getBytes();

    /**
     * The value as the backend decoded it, no conversion applied, or {@code null} if the cell is null.
     * Always a JDK type; a CQL type with no JDK equivalent (a UDT, a tuple, {@code duration}) throws rather
     * than let a driver class cross this boundary - use {@link CQLExecutionService#query} for those.
     *
     * @throws UnsupportedOperationException if the column's type has no JDK representation
     */
    Object getObject();
}
