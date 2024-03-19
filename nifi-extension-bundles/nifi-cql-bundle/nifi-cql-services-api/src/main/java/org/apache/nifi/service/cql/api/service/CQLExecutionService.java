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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.service.cql.api.constants.ConnectionCompression;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.constants.CqlConsistencyLevel;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;
import org.apache.nifi.service.cql.api.metadata.PrimaryKey;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.ssl.SSLContextService;

import java.util.List;
import java.util.Map;

/**
 * Backend-agnostic connection service for Apache Cassandra/ScyllaDB, so {@code PutCQLRecord},
 * {@code ExecuteCQLQueryRecord} and {@code CQLDistributedMapCache} need not know which driver
 * implementation is configured. The property descriptors are the shared connection settings both
 * implementations expose identically; the methods are the query/write contract each must provide.
 */
public interface CQLExecutionService extends ControllerService {
    PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("Contact points are addresses of Cassandra nodes, as a comma-separated list of "
                    + "hostname:port entries - for example node1:9042,node2:9042. An IPv6 address must be "
                    + "bracketed to carry a port, as [::1]:9042. If an entry names no port, the default "
                    + "client port of 9042 is used.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(ContactPoints.VALIDATOR)
            .build();

    PropertyDescriptor DATACENTER = new PropertyDescriptor.Builder()
            .name("Cassandra Datacenter")
            .description("The datacenter setting to use with your node/cluster.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Default Keyspace")
            .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to " +
                    "include the keyspace name before any table reference, in case of 'query' native processors or " +
                    "if the processor supports the 'Table' property, the keyspace name has to be provided with the " +
                    "table name in the form of <KEYSPACE>.<TABLE>")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(true)
            .allowableValues(CqlConsistencyLevel.class)
            .defaultValue(CqlConsistencyLevel.ONE.getValue())
            .build();

    PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch size")
            .description("The number of result rows to be fetched from the result set at a time - the page size the "
                    + "driver requests per round trip, not a cap on total rows returned. Zero is the default and means "
                    + "the driver's own default page size (5000) is used.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Enable compression at transport-level requests and responses")
            .required(false)
            .allowableValues(ConnectionCompression.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(ConnectionCompression.NONE.getValue())
            .build();

    PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Read timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Connection timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    PropertyDescriptor DEFAULT_TTL = new PropertyDescriptor.Builder()
            .name("Default Time To Live")
            .description("The default Time To Live (TTL) to apply to INSERT statements and SET-method UPDATE statements when a "
                    + "processor does not override it. Counter UPDATE statements never have a TTL applied, since Cassandra/ScyllaDB do not "
                    + "support one on counter columns. If not set, no TTL is applied and the table's own default_time_to_live (if any) governs.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    /**
     * File settings override the other connection properties; anything the file omits falls back to them.
     * This is the {@code DriverConfigLoader.compose(file, properties)} precedence of the DataStax Java
     * Driver, which ScyllaDB's shard-aware fork shares along with the HOCON format.
     */
    PropertyDescriptor DRIVER_CONFIGURATION_FILE = new PropertyDescriptor.Builder()
            .name("Driver Configuration File")
            .description("Path to an optional Java Driver configuration file using the driver's standard "
                    + "typesafe-config (HOCON) format, for example an application.conf providing advanced "
                    + "settings such as load balancing policy, retry policy, or speculative execution that "
                    + "are not otherwise exposed as properties on this service. Settings in this file take "
                    + "precedence over the other connection properties configured here; any setting not "
                    + "present in the file falls back to those properties or the driver's built-in defaults. "
                    + "ScyllaDB's Java Driver is a shard-aware fork of the DataStax Java Driver that reads the "
                    + "same configuration format, so this file works unchanged when connecting to ScyllaDB.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

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
