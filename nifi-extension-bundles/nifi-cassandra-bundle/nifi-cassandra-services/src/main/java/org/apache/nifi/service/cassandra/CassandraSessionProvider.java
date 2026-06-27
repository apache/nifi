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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.cassandra.CassandraClient;
import org.apache.nifi.cassandra.CassandraConnectionService;
import org.apache.nifi.cassandra.CompressionType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

@Tags({ "cassandra", "dbcp", "database", "connection", "pooling" })
@CapabilityDescription("Provides connection session for Cassandra processors to work with Apache Cassandra.")
public class CassandraSessionProvider extends AbstractControllerService implements CassandraConnectionService {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    private static final String[] CONSISTENCY_LEVELS = {
            ConsistencyLevel.ANY.name(),
            ConsistencyLevel.ONE.name(),
            ConsistencyLevel.TWO.name(),
            ConsistencyLevel.THREE.name(),
            ConsistencyLevel.QUORUM.name(),
            ConsistencyLevel.ALL.name(),
            ConsistencyLevel.LOCAL_ONE.name(),
            ConsistencyLevel.LOCAL_QUORUM.name(),
            ConsistencyLevel.EACH_QUORUM.name(),
            ConsistencyLevel.SERIAL.name(),
            ConsistencyLevel.LOCAL_SERIAL.name()
    };

    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("""
                    Contact points are addresses of Cassandra nodes. The list of contact points should be
                    comma-separated and in hostname:port format. Example node1:port,node2:port,....
                    The default client port for Cassandra is 9042, but the port(s) must be explicitly specified.
                    """)
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Keyspace")
            .description("""
                    The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to include
                    keyspace name before any table reference. For query-native processors, or processors that support
                    the Table property, provide the table as <KEYSPACE>.<TABLE>.
                    """)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("""
                    The SSL Context Service used to provide the client certificate information for establishing secure
                    TLS/SSL connections to the Cassandra cluster.
                    """)
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(true)
            .allowableValues(CONSISTENCY_LEVELS)
            .defaultValue(ConsistencyLevel.QUORUM.name())
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("""
                    Specifies the compression type used for transport-level requests and responses.Note that Snappy
                    compression is not supported when using Protocol V5.
                    """)
            .required(false)
            .allowableValues(CompressionType.class)
            .defaultValue(CompressionType.NONE.getValue())
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Read timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Connection timeout. 0 means no timeout. If no value is set, the underlying default will be used.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOCAL_DATACENTER = new PropertyDescriptor.Builder()
            .name("Local Datacenter")
            .description("The local datacenter name for the Cassandra cluster (required by driver 4.x).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private CqlSession cassandraSession;
    private CassandraClient cassandraClient;
    private String clusterAddress;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
                    CONTACT_POINTS,
                    CONSISTENCY_LEVEL,
                    COMPRESSION_TYPE,
                    KEYSPACE,
                    USERNAME,
                    PASSWORD,
                    PROP_SSL_CONTEXT_SERVICE,
                    READ_TIMEOUT,
                    CONNECT_TIMEOUT,
                    LOCAL_DATACENTER
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            connectToCassandra(context);
        } catch (Exception e) {
            throw new InitializationException("Failed to connect to Cassandra cluster. Please check your configuration.", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (cassandraSession != null) {
            cassandraSession.close();
            cassandraSession = null;
        }
        cassandraClient = null;
    }

    @Override
    public CassandraClient getClient() {
        if (cassandraClient == null) {
            throw new ProcessException("""
                Cassandra Client is not initialized. The Controller Service failed to connect to the cluster.
            """);
        }
        return cassandraClient;
    }

    void connectToCassandra(ConfigurationContext context) {
        if (cassandraSession == null) {
            ComponentLog log = getLogger();

            final String contactPointList = context.getProperty(CONTACT_POINTS).getValue();
            final String keyspaceName = context.getProperty(KEYSPACE).getValue();
            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).getValue();
            final CompressionType compressionType = context.getProperty(COMPRESSION_TYPE)
                    .asAllowableValue(CompressionType.class);

            this.clusterAddress = contactPointList + "/" + keyspaceName;
            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            final SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE)
                            .asControllerService(SSLContextService.class);
            final SSLContext sslContext = (sslService != null) ? sslService.createContext() : null;

            final Optional<Long> readTimeoutMs = Optional.ofNullable(context.getProperty(READ_TIMEOUT))
                            .filter(PropertyValue::isSet)
                            .map(value -> value.asTimePeriod(TimeUnit.MILLISECONDS));

            final Optional<Long> connectTimeoutMs = Optional
                            .ofNullable(context.getProperty(CONNECT_TIMEOUT))
                            .filter(PropertyValue::isSet)
                            .map(value -> value.asTimePeriod(TimeUnit.MILLISECONDS));

            final String localDatacenter = context.getProperty(LOCAL_DATACENTER).getValue();

            if (localDatacenter == null || localDatacenter.isBlank()) {
                throw new ProcessException("Local Datacenter must be configured");
            }

            String consistencyLevels = context.getProperty(CONSISTENCY_LEVEL).evaluateAttributeExpressions().getValue();

            String compressionValue = (compressionType != null)
                            ? compressionType.getValue().toLowerCase()
                            : CompressionType.NONE.getValue().toLowerCase();

            ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder();

            readTimeoutMs.ifPresent(timeout -> {
                if (timeout > 0) {
                    loaderBuilder.withDuration(
                                    DefaultDriverOption.REQUEST_TIMEOUT,
                                    Duration.ofMillis(timeout));
                }
            });

            connectTimeoutMs.ifPresent(timeout -> {
                if (timeout > 0) {
                    loaderBuilder.withDuration(
                            DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
                            Duration.ofMillis(timeout));
                }
            });

            loaderBuilder.withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevels)
                            .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compressionValue);

            DriverConfigLoader loader = loaderBuilder.build();

            CqlSessionBuilder builder = CqlSession.builder()
                                                .addContactPoints(contactPoints)
                                                .withLocalDatacenter(localDatacenter)
                                                .withConfigLoader(loader);

            if (keyspaceName != null && !keyspaceName.isEmpty()) {
                builder = builder.withKeyspace(CqlIdentifier.fromCql(keyspaceName));
            }

            if (username != null && password != null) {
                builder = builder.withAuthCredentials(username, password);
            }

            if (sslContext != null) {
                builder = builder.withSslContext(sslContext);
            }

            cassandraSession = builder.build();
            cassandraClient = new StandardCassandraClient(cassandraSession);

            Metadata metadata = cassandraSession.getMetadata();

            if (keyspaceName != null && !keyspaceName.isEmpty()) {
                if (metadata.getKeyspace(CqlIdentifier.fromCql(keyspaceName)).isEmpty()) {
                    cassandraSession.close();
                    throw new ProcessException("Keyspace with name '" + keyspaceName + "' does not exist in the Cassandra cluster.");
                }
            }

            boolean dcExists = metadata.getNodes().values().stream()
                    .map(Node::getDatacenter)
                    .anyMatch(localDatacenter::equals);

            if (!dcExists) {
                cassandraSession.close();
                throw new ProcessException("Local Datacenter '" + localDatacenter + "' does not exist in the Cassandra cluster.");
            }

            log.info("Connected to Cassandra session: {}", metadata.getClusterName());
        }
    }

    private List<InetSocketAddress> getContactPoints(String contactPointList) {

        if (contactPointList == null) {
            return null;
        }

        final String[] contactPointStringList = contactPointList.split(",");
        List<InetSocketAddress> contactPoints = new ArrayList<>();

        for (String contactPointEntry : contactPointStringList) {
            String[] addresses = contactPointEntry.split(":");
            final String hostName = addresses[0].trim();
            final int port = (addresses.length > 1) ? Integer.parseInt(addresses[1].trim())
                                : DEFAULT_CASSANDRA_PORT;

            contactPoints.add(new InetSocketAddress(hostName, port));
        }

        return contactPoints;
    }

    @Override
    public String getDatabaseLocation() {
        return clusterAddress;
    }

}
