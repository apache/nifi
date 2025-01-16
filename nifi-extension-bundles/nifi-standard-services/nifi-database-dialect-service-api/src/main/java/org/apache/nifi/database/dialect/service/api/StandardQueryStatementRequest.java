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
package org.apache.nifi.database.dialect.service.api;

import java.util.Optional;

/**
 * Standard implementation of Query Statement Request with required properties
 *
 * @param statementType SQL Statement Type
 * @param tableDefinition Database Table Definition
 * @param derivedTable Derived Table Query or empty when not defined
 * @param whereClause SQL WHERE clause or empty when not defined
 * @param orderByClause SQL ORDER BY clause or empty when not defined
 * @param pageRequest Page Request can be empty
 */
public record StandardQueryStatementRequest(
        StatementType statementType,
        TableDefinition tableDefinition,
        Optional<String> derivedTable,
        Optional<String> whereClause,
        Optional<String> orderByClause,
        Optional<PageRequest> pageRequest
) implements QueryStatementRequest {
    /**
     * Standard Query Statement Request without additional clauses
     *
     * @param statementType Statement Type
     * @param tableDefinition Database Table Definition
     */
    public StandardQueryStatementRequest(final StatementType statementType, final TableDefinition tableDefinition) {
        this(statementType, tableDefinition, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }
}
