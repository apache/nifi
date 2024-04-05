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
package org.apache.nifi.questdb;

/**
 * Wraps the necessary services of QuestDb.
 */
public interface Client {
    /**
     * Executes a query not caring about the possible return values. Primarily for DDL commands.
     *
     * @param query The QuestDB query to execute.
     */
    void execute(String query) throws DatabaseException;

    /**
     * Inserts new rows into the database using a source object for specifying the inserted data.
     *
     * @param tableName The target table.
     * @param rowSource Specifies the row data to be inserted.
     */
    void insert(
        String tableName,
        InsertRowDataSource rowSource
    ) throws DatabaseException;

    /**
     *
     * Queries information from the database which will be handled by a row processor. This row processor depending on the
     * implementation might create an entry for every row in the query result ({@code QuestDbRowProcessor#forMapping} or
     * potentially create an aggregate.
     *
     * @param query The query string.
     * @param rowProcessor Maps the query result into a presentation format.
     *
     * @return The result of the query.
     *
     * @param <T> Result type.
     */
    <T> T query(
        String query,
        QueryResultProcessor<T> rowProcessor
    ) throws DatabaseException;

    /**
     * Terminates the client. After {@code disconnect} is called, answer for other calls is not guaranteed.
     */
    void disconnect() throws DatabaseException;
}
