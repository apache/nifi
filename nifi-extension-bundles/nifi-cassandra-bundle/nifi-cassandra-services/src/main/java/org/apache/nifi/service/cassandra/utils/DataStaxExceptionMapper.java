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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import org.apache.nifi.cassandra.exception.CassandraException;
import org.apache.nifi.cassandra.exception.CassandraExceptionCategory;

public final class DataStaxExceptionMapper {

    public CassandraException map(final String query, final Throwable throwable) {
        final Throwable cause = getRootCause(throwable);
        final CassandraExceptionCategory retry = CassandraExceptionCategory.RETRY;
        final CassandraExceptionCategory failure = CassandraExceptionCategory.FAILURE;

        if (cause instanceof AllNodesFailedException) {
            return new CassandraException("No node in the Cassandra cluster can be contacted to execute the query", retry, cause);
        }
        if (cause instanceof DriverTimeoutException) {
            return new CassandraException("Cassandra query timed out", retry, cause);
        }
        if (cause instanceof QueryValidationException) {
            return new CassandraException("The CQL query [" + query + "] is invalid due to syntax error or validation problem", failure, cause);
        }
        if (cause instanceof QueryExecutionException) {
            return new CassandraException("Cassandra query execution failed for [" + query + "]", retry, cause);
        }

        return new CassandraException("Failed to execute CQL query [" + query + "] due to an unexpected error", failure, cause);
    }

    private Throwable getRootCause(final Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
