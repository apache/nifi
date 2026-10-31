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
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.service.cql.api.constants.ConnectionCompression;
import org.apache.nifi.service.cql.api.constants.CqlConsistencyLevel;
import org.apache.nifi.ssl.SSLContextProvider;

/**
 * Base class for {@link CQLExecutionService} implementations. Holds the shared connection property
 * descriptors both the Cassandra and ScyllaDB services expose identically, so a subclass only declares the
 * order it lists them in and the driver-specific wiring behind them.
 */
public abstract class AbstractCQLExecutionService extends AbstractControllerService {

    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("""
                    Contact points are addresses of Cassandra nodes, as a comma-separated list of \
                    hostname:port entries - for example node1:9042,node2:9042. An IPv6 address must be \
                    bracketed to carry a port, as [::1]:9042. If an entry names no port, the default \
                    client port of 9042 is used.""")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(ContactPoints.VALIDATOR)
            .build();

    public static final PropertyDescriptor DATACENTER = new PropertyDescriptor.Builder()
            .name("Cassandra Datacenter")
            .description("The datacenter setting to use with your node/cluster.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Default Keyspace")
            .description("""
                    The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to \
                    include the keyspace name before any table reference, in case of 'query' native processors or \
                    if the processor supports the 'Table' property, the keyspace name has to be provided with the \
                    table name in the form of <KEYSPACE>.<TABLE>""")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("""
                    The SSL Context Service used to provide client certificate information for TLS/SSL \
                    connections.""")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(true)
            .allowableValues(CqlConsistencyLevel.class)
            .defaultValue(CqlConsistencyLevel.ONE.getValue())
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch size")
            .description("""
                    The number of result rows to be fetched from the result set at a time - the page size the \
                    driver requests per round trip, not a cap on total rows returned. Zero is the default and means \
                    the driver's own default page size (5000) is used.""")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Enable compression at transport-level requests and responses")
            .required(false)
            .allowableValues(ConnectionCompression.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(ConnectionCompression.NONE.getValue())
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Read timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Connection timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEFAULT_TTL = new PropertyDescriptor.Builder()
            .name("Default Time To Live")
            .description("""
                    The default Time To Live (TTL) to apply to INSERT statements and SET-method UPDATE statements when a \
                    processor does not override it. Counter UPDATE statements never have a TTL applied, since Cassandra/ScyllaDB do not \
                    support one on counter columns. If not set, no TTL is applied and the table's own default_time_to_live (if any) governs.""")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    /**
     * File settings override the other connection properties; anything the file omits falls back to them.
     * This is the {@code DriverConfigLoader.compose(file, properties)} precedence of the DataStax Java
     * Driver, which ScyllaDB's shard-aware fork shares along with the HOCON format.
     */
    public static final PropertyDescriptor DRIVER_CONFIGURATION_FILE = new PropertyDescriptor.Builder()
            .name("Driver Configuration File")
            .description("""
                    Path to an optional Java Driver configuration file using the driver's standard \
                    typesafe-config (HOCON) format, for example an application.conf providing advanced \
                    settings such as load balancing policy, retry policy, or speculative execution that \
                    are not otherwise exposed as properties on this service. Settings in this file take \
                    precedence over the other connection properties configured here; any setting not \
                    present in the file falls back to those properties or the driver's built-in defaults. \
                    ScyllaDB's Java Driver is a shard-aware fork of the DataStax Java Driver that reads the \
                    same configuration format, so this file works unchanged when connecting to ScyllaDB.""")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
}
