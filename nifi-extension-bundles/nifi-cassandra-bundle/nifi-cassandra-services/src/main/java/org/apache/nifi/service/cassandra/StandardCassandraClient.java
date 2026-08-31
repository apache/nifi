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
package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import org.apache.nifi.cassandra.CassandraClient;
import org.apache.nifi.cassandra.exception.CassandraException;
import org.apache.nifi.cassandra.models.CassandraQueryRequest;
import org.apache.nifi.cassandra.models.CassandraQueryResult;
import org.apache.nifi.service.cassandra.utils.DataStaxExceptionMapper;
import org.apache.nifi.service.cassandra.utils.DataStaxTypeMapper;
import org.apache.nifi.service.cassandra.utils.StandardCassandraQueryResult;

import java.util.concurrent.ExecutionException;

final class StandardCassandraClient implements CassandraClient {
    private final CqlSession cqlSession;
    private final DataStaxTypeMapper typeMapper = new DataStaxTypeMapper();
    private final DataStaxExceptionMapper exceptionMapper = new DataStaxExceptionMapper();

    StandardCassandraClient(final CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    @Override
    public CassandraQueryResult executeQuery(final CassandraQueryRequest request) throws CassandraException {
        try {
            final SimpleStatementBuilder statementBuilder = new SimpleStatementBuilder(request.cql());
            if (request.fetchSize() != null) {
                statementBuilder.setPageSize(request.fetchSize());
            }

            if (request.timeout() != null) {
                statementBuilder.setTimeout(request.timeout());
            }

            final AsyncResultSet asyncResultSet = cqlSession.executeAsync(statementBuilder.build())
                    .toCompletableFuture().get();

            return new StandardCassandraQueryResult(asyncResultSet, typeMapper, exceptionMapper, request.cql());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw exceptionMapper.map(request.cql(), e);
        } catch (ExecutionException | RuntimeException e) {
            throw exceptionMapper.map(request.cql(), e);
        }
    }
}
