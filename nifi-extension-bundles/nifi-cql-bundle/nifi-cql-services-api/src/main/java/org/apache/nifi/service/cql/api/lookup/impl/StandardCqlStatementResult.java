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
package org.apache.nifi.service.cql.api.lookup.impl;

import org.apache.nifi.service.cql.api.lookup.CqlRow;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;

import java.util.List;
import java.util.Objects;

/**
 * A {@link CqlStatementResult} over a fixed outcome and row list.
 *
 * @param wasApplied whether a conditional statement applied; {@code true} for an unconditional one
 * @param rows       the rows the server returned, in order; copied, non-null
 */
public record StandardCqlStatementResult(boolean wasApplied, List<CqlRow> rows) implements CqlStatementResult {

    /** An outcome with no rows, for a statement that returned none. */
    public static final CqlStatementResult APPLIED_WITH_NO_ROWS = new StandardCqlStatementResult(true, List.of());

    public StandardCqlStatementResult {
        rows = List.copyOf(Objects.requireNonNull(rows, "rows is required"));
    }
}
