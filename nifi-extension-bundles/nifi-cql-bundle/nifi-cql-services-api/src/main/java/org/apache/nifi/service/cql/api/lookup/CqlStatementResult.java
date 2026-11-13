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

import java.util.List;

/**
 * The outcome of a statement executed through {@link CQLExecutionService#execute}.
 */
public interface CqlStatementResult {

    /**
     * Whether a conditional statement ({@code IF NOT EXISTS}, {@code IF EXISTS}, {@code IF <condition>})
     * applied. Always {@code true} for an unconditional one, so a caller that never writes conditionally has
     * no special case. The backends report this as an {@code [applied]} column - not a legal identifier -
     * which is surfaced here as an outcome and excluded from {@link #rows()}.
     */
    boolean wasApplied();

    /**
     * The rows the server returned; never {@code null}, often empty.
     *
     * <p>A <em>rejected</em> conditional write returns the row that is actually stored, letting a
     * compare-and-set report the current value in one round trip. An applied conditional write and an
     * unconditional write return no rows; a {@code SELECT} returns its selected rows. Implementations must
     * normalize the empty-when-applied case, which the backends otherwise report differently.
     */
    List<CqlRow> rows();
}
