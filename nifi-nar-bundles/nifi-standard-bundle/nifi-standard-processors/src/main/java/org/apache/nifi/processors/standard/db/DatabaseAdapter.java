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
package org.apache.nifi.processors.standard.db;

/**
 * Interface for RDBMS/JDBC-specific code.
 */
public interface DatabaseAdapter {

    String getName();

    String getDescription();

    /**
     * Returns a SQL SELECT statement with the given clauses applied.
     *
     * @param tableName     The name of the table to fetch rows from
     * @param columnNames   The names of the columns to fetch from the table
     * @param whereClause   The filter to apply to the statement. This should not include the WHERE keyword
     * @param orderByClause The columns/clause used for ordering the result rows. This should not include the ORDER BY keywords
     * @param limit         The value for the LIMIT clause (i.e. the number of rows to return)
     * @param offset        The value for the OFFSET clause (i.e. the number of rows to skip)
     * @return A String containing a SQL SELECT statement with the given clauses applied
     */
    String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset);

    /**
     * Returns a SQL SELECT statement with the given clauses applied. Note that if this method is overridden, the other overloaded methods
     * need to be overridden as well, to call this method with columnForPartitioning = false
     *
     * @param tableName             The name of the table to fetch rows from
     * @param columnNames           The names of the columns to fetch from the table
     * @param whereClause           The filter to apply to the statement. This should not include the WHERE keyword
     * @param orderByClause         The columns/clause used for ordering the result rows. This should not include the ORDER BY keywords
     * @param limit                 The value for the LIMIT clause (i.e. the number of rows to return)
     * @param offset                The value for the OFFSET clause (i.e. the number of rows to skip)
     * @param columnForPartitioning The (optional) column name that, if provided, the limit and offset values are based on values from the column itself (rather than the row number)
     * @return A String containing a SQL SELECT statement with the given clauses applied
     */
    default String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset);
    }

    /**
     * <p>Returns a bare identifier string by removing wrapping escape characters
     * from identifier strings such as table and column names.</p>
     * <p>The default implementation of this method removes double quotes.
     * If the target database engine supports different escape characters, then its DatabaseAdapter implementation should override
     * this method so that such escape characters can be removed properly.</p>
     * @param identifier An identifier which may be wrapped with escape characters
     * @return An unwrapped identifier string, or null if the input identifier is null
     */
    default String unwrapIdentifier(String identifier) {
        return identifier == null ? null : identifier.replaceAll("\"", "");
    }

    default String getTableAliasClause(String tableName) {
        return "AS " + tableName;
    }
}
