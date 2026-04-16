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
package org.apache.nifi.service.cassandra.utils;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import org.apache.nifi.cassandra.exception.CassandraException;
import org.apache.nifi.cassandra.models.CassandraColumnDefinition;
import org.apache.nifi.cassandra.models.CassandraQueryResult;
import org.apache.nifi.cassandra.models.CassandraRow;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

public final class StandardCassandraQueryResult implements CassandraQueryResult {
    private final AsyncResultSet asyncResultSet;
    private final DataStaxTypeMapper typeMapper;
    private final DataStaxExceptionMapper exceptionMapper;
    private final String query;
    private final List<CassandraColumnDefinition> columnDefinitions;

    public StandardCassandraQueryResult(
            final AsyncResultSet asyncResultSet,
            final DataStaxTypeMapper typeMapper,
            final DataStaxExceptionMapper exceptionMapper,
            final String query) {
        this.asyncResultSet = asyncResultSet;
        this.typeMapper = typeMapper;
        this.exceptionMapper = exceptionMapper;
        this.query = query;
        this.columnDefinitions = StreamSupport.stream(asyncResultSet.getColumnDefinitions().spliterator(), false)
                .map(this::mapColumnDefinition)
                .toList();
    }

    @Override
    public List<CassandraColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    @Override
    public Iterable<CassandraRow> getCurrentPage() {
        return () -> StreamSupport.stream(asyncResultSet.currentPage().spliterator(), false)
                .map(row -> (CassandraRow) new StandardCassandraRow(row, typeMapper))
                .iterator();
    }

    @Override
    public boolean hasMorePages() {
        return asyncResultSet.hasMorePages();
    }

    @Override
    public CassandraQueryResult fetchNextPage() throws CassandraException {
        try {
            final AsyncResultSet nextPage = asyncResultSet.fetchNextPage().toCompletableFuture().get();
            return new StandardCassandraQueryResult(nextPage, typeMapper, exceptionMapper, query);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw exceptionMapper.map(query, e);
        } catch (ExecutionException | RuntimeException e) {
            throw exceptionMapper.map(query, e);
        }
    }

    private CassandraColumnDefinition mapColumnDefinition(final ColumnDefinition columnDefinition) {
        return new CassandraColumnDefinition(
                columnDefinition.getName().asInternal(),
                typeMapper.map(columnDefinition.getType()));
    }
}
