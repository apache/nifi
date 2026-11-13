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
package org.apache.nifi.service.cql.api.service;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;
import org.apache.nifi.service.cql.api.metadata.PrimaryKey;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;

import java.util.List;
import java.util.Map;

/**
 * Backend-agnostic connection service for Apache Cassandra/ScyllaDB, so {@code PutCQLRecord},
 * {@code ExecuteCQLQueryRecord} and {@code CQLDistributedMapCache} need not know which driver
 * implementation is configured. The shared connection property descriptors live on
 * {@link AbstractCQLExecutionService}; the methods here are the query/write contract each implementation
 * must provide.
 */
public interface CQLExecutionService extends ControllerService {

    /**
     * Executes a statement (typically a {@code SELECT}) and streams rows to {@code callback} one at a time,
     * so arbitrarily large result sets are supported. Supply bind-marker values through {@code parameters}
     * rather than interpolating them into {@code cql}, so untrusted input is sent as data, never parsed as
     * CQL.
     *
     * @param cql statement text with {@code ?} bind markers
     * @param parameters positional bind values, in order, or {@code null}/empty if none; each is converted
     * to the Java type its bind marker's CQL type requires, so text still binds to a non-text column
     * @param callback invoked once per row; the last row has {@code hasMore == false}
     * @param overrides per-call fetch size/timeout; {@link QueryOverrides#NONE} for none
     * @throws QueryFailureException on a driver/server-level failure (malformed input may instead be unchecked)
     */
    void query(String cql, List<Object> parameters, CQLQueryCallback callback, QueryOverrides overrides) throws QueryFailureException;

    /**
     * Executes a statement (not only {@code SELECT}) and returns its outcome. The counterpart to
     * {@link #query} for two things a per-row callback cannot express: the applied/rejected outcome of a
     * conditional write, and reading values with no schema imposed. Materializes its rows, so it is for
     * bounded results (conditional writes, single-row lookups, existence probes); use {@link #query} for
     * large result sets.
     *
     * @param cql statement text with {@code ?} bind markers
     * @param parameters positional bind values, in order, converted as in {@link #query}, or {@code null}/empty if none
     * @param overrides per-call fetch size/timeout; {@link QueryOverrides#NONE} for none
     * @return the outcome; never {@code null}
     * @throws QueryFailureException on a driver/server-level failure
     */
    CqlStatementResult execute(String cql, List<Object> parameters, QueryOverrides overrides) throws QueryFailureException;

    /**
     * Writes one record to {@code table} via {@code INSERT}. Each field maps to the column of the same name,
     * primary key columns included, unless {@code primaryKeyOverrides} redirects one.
     *
     * @param table target table, keyspace-qualified or unqualified against the connection's default keyspace
     * @param record the record to write
     * @param primaryKeyOverrides maps a {@link PrimaryKeyIdentifier} to a {@link RecordPath} that resolves
     * that column's value from the record (must select exactly one value) instead of a same-named field
     * @param overrides per-call TTL/write-timestamp; {@link WriteOverrides#NONE} for none
     * @throws QueryFailureException on a driver/server-level failure
     */
    void insert(QualifiedTableName table, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                WriteOverrides overrides) throws QueryFailureException;

    /**
     * Batch form of {@link #insert(QualifiedTableName, org.apache.nifi.serialization.record.Record, Map, WriteOverrides)}:
     * one {@code BatchStatement} of {@code batchType} over all {@code records}, which must share a schema
     * (one prepared statement is built from the first and reused). {@code null}/empty {@code records} is a no-op.
     *
     * @throws QueryFailureException on a driver/server-level failure
     */
    void insert(QualifiedTableName table, List<org.apache.nifi.serialization.record.Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                CqlBatchType batchType, WriteOverrides overrides) throws QueryFailureException;

    /**
     * @param tableName the table a write targeted
     * @return a provenance identifier for that write; not a dereferenceable URL
     */
    String getTransitUrl(QualifiedTableName tableName);

    /**
     * Deletes the row identified by {@code updateKeys} from {@code cassandraTable}.
     *
     * @param cassandraTable target table, keyspace-qualified or unqualified against the default keyspace
     * @param record supplies each {@code updateKeys} column's value by matching field name
     * @param primaryKeyOverrides as in {@link #insert}
     * @param updateKeys column names identifying the row; non-empty, each a field in {@code record}
     * @throws QueryFailureException on a driver/server-level failure
     */
    void delete(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                List<String> updateKeys) throws QueryFailureException;

    /**
     * Writes one record to {@code cassandraTable} via {@code UPDATE}: {@code updateKeys} form the
     * {@code WHERE} clause, every other field is applied per {@code updateMethod}.
     *
     * @param cassandraTable target table, keyspace-qualified or unqualified against the default keyspace
     * @param record the record to write
     * @param primaryKeyOverrides as in {@link #insert}
     * @param updateKeys column names identifying the row; non-empty, each a field in {@code record}
     * @param updateMethod {@code SET}, or {@code INCREMENT}/{@code DECREMENT} for a counter column
     * @param overrides per-call TTL/write-timestamp; ignored for {@code INCREMENT}/{@code DECREMENT}, which
     * take neither a TTL nor a custom write timestamp
     * @throws QueryFailureException on a driver/server-level failure
     */
    void update(QualifiedTableName cassandraTable, org.apache.nifi.serialization.record.Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                List<String> updateKeys, UpdateMethod updateMethod, WriteOverrides overrides) throws QueryFailureException;

    /**
     * Batch form of {@link #update(QualifiedTableName, org.apache.nifi.serialization.record.Record, Map, List, UpdateMethod, WriteOverrides)}:
     * one {@code BatchStatement} of {@code batchType} over all {@code records}, which must share a schema.
     * {@code INCREMENT}/{@code DECREMENT} require a {@code COUNTER} or {@code UNLOGGED} batch.
     * {@code null}/empty {@code records} is a no-op.
     *
     * @throws QueryFailureException on a driver/server-level failure
     */
    void update(QualifiedTableName cassandraTable, List<org.apache.nifi.serialization.record.Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                List<String> updateKeys, UpdateMethod updateMethod,
                CqlBatchType batchType, WriteOverrides overrides) throws QueryFailureException;

    /**
     * @param table the table to describe, keyspace-qualified or unqualified against the default keyspace
     * @return {@code table}'s primary key (partition and clustering columns, in declared order), from the
     * cluster's schema metadata
     */
    PrimaryKey getMetadata(QualifiedTableName table);

    /**
     * @return the default keyspace configured on this service, or {@code null} if none
     */
    String getDefaultKeyspace();
}
