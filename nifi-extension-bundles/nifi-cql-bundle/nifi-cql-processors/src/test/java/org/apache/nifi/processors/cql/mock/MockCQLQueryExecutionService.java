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

package org.apache.nifi.processors.cql.mock;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;
import org.apache.nifi.service.cql.api.metadata.PrimaryKey;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.CQLQueryCallback;
import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.apache.nifi.service.cql.api.service.WriteOverrides;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This test class exists to support @see org.apache.nifi.processors.cassandra.ExecuteCQLQueryRecordTest
 */
public class MockCQLQueryExecutionService extends AbstractControllerService implements CQLExecutionService {
    /**
     * Stands in for a DataStax page-fetch exception. The driver types cannot be referenced from this module,
     * and they never reach a processor anyway - a real service translates them before they cross the
     * {@link CQLExecutionService} boundary.
     */
    private static class SimulatedPageFetchFailure extends RuntimeException {
    }

    /** Sentinel for {@link #rowsBeforeFailure} meaning "deliver the whole result set successfully". */
    private static final int NEVER_FAIL = -1;

    private final Iterator<Record> records;
    private QueryOverrides lastQueryOverrides;
    private String lastCql;
    private List<Object> lastParameters;
    private int rowsBeforeFailure = NEVER_FAIL;

    public MockCQLQueryExecutionService(Iterator<Record> records) {
        this.records = records;
    }

    /**
     * Makes {@link #query} deliver {@code rowsBeforeFailure} rows and then fail, the way a page fetch does
     * partway through a large result set. Every row delivered in this mode reports {@code hasMore = true},
     * since by definition the result set was not exhausted. Pass {@code 0} to fail before any row arrives.
     *
     * @return this service, for chaining onto the constructor
     */
    public MockCQLQueryExecutionService failAfter(final int rowsBeforeFailure) {
        this.rowsBeforeFailure = rowsBeforeFailure;
        return this;
    }

    /**
     * Unimplemented on purpose: nothing in {@code nifi-cql-processors} calls {@code execute} - it exists for
     * callers reading values without a schema over them, such as a distributed map cache - so a mock return
     * value here would only be a guess at behaviour no test exercises. Throwing keeps that honest, and makes
     * the first processor to need it fail loudly rather than against a fiction.
     */
    @Override
    public CqlStatementResult execute(String cql, List<Object> parameters, QueryOverrides overrides) {
        throw new UnsupportedOperationException("execute() is not stubbed; no processor test needs it yet");
    }

    /**
     * Mirrors the structure of a real implementation's {@code query}, not just its happy path: the entire row
     * loop sits inside the try, so an exception thrown by the <em>callback</em> - a record writer that fails
     * mid-result-set, say - is caught here too, gets {@link CQLQueryCallback#clear()} called on it, and is
     * rethrown as a {@link ProcessException}. Cleanup on that path is easy to get wrong, and a mock that let
     * the callback's exception escape untouched would never exercise it.
     */
    @Override
    public void query(String cql, List<Object> parameters, CQLQueryCallback callback, QueryOverrides overrides) throws QueryFailureException {
        this.lastQueryOverrides = overrides;
        this.lastCql = cql;
        this.lastParameters = parameters;

        final boolean failing = rowsBeforeFailure != NEVER_FAIL;

        try {
            long rowNumber = 0;

            while (records.hasNext() && (!failing || rowNumber < rowsBeforeFailure)) {
                final Record next = records.next();
                callback.receive(++rowNumber, next, failing || records.hasNext());
            }

            if (failing) {
                throw new SimulatedPageFetchFailure();
            }
        } catch (final SimulatedPageFetchFailure e) {
            callback.clear();
            throw new QueryFailureException();
        } catch (final Exception ex) {
            callback.clear();
            throw new ProcessException("Error querying CQL", ex);
        }
    }

    public QueryOverrides getLastQueryOverrides() {
        return lastQueryOverrides;
    }

    public String getLastCql() {
        return lastCql;
    }

    public List<Object> getLastParameters() {
        return lastParameters;
    }

    @Override
    public void insert(QualifiedTableName table, Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(QualifiedTableName table, List<Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, CqlBatchType batchType, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTransitUrl(QualifiedTableName tableName) {
        return "";
    }

    @Override
    public void delete(QualifiedTableName cassandraTable, Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(QualifiedTableName cassandraTable, Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                       List<String> updateKeys, UpdateMethod updateMethod, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(QualifiedTableName cassandraTable, List<Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> updateKeys,
                       UpdateMethod updateMethod, CqlBatchType batchType, WriteOverrides overrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimaryKey getMetadata(QualifiedTableName table) {
        return null;
    }

    @Override
    public String getDefaultKeyspace() {
        return "nifi_test";
    }
}
