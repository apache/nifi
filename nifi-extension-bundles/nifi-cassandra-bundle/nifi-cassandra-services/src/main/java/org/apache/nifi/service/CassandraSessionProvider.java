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
package org.apache.nifi.service;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import io.netty.handler.ssl.ClientAuth;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
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
import javax.net.ssl.SSLContext;


@Tags({"cassandra", "dbcp", "database", "connection", "pooling"})
@CapabilityDescription("Provides connection session for Cassandra processors to work with Apache Cassandra.")
public class CassandraSessionProvider extends AbstractControllerService implements CassandraSessionProviderService {

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

    // Common descriptors
    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("Contact points are addresses of Cassandra nodes. The list of contact points should be "
                    + "comma-separated and in hostname:port format. Example node1:port,node2:port,...."
                    + " The default client port for Cassandra is 9042, but the port(s) must be explicitly specified.")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Keyspace")
            .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to " +
                    "include the keyspace name before any table reference, in case of 'query' native processors or " +
                    "if the processor supports the 'Table' property, the keyspace name has to be provided with the " +
                    "table name in the form of <KEYSPACE>.<TABLE>")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue("REQUIRE")
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

    static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Enable compression at transport-level requests and responses. Snappy compression is not supported in Protocol V5.")
            .required(false)
            .allowableValues(CompressionType.class)
            .defaultValue(CompressionType.NONE.getValue())
            .build();

    static final PropertyDescriptor READ_TIMEOUT_MS = new PropertyDescriptor.Builder()
        .name("Read Timeout (ms)")
        .description("Read timeout (in milliseconds). 0 means no timeout. If no value is set, the underlying default will be used.")
        .required(false)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build();

    static final PropertyDescriptor CONNECT_TIMEOUT_MS = new PropertyDescriptor.Builder()
        .name("Connect Timeout (ms)")
        .description("Connection timeout (in milliseconds). 0 means no timeout. If no value is set, the underlying default will be used.")
        .required(false)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor LOCAL_DATACENTER = new PropertyDescriptor.Builder()
            .name("Local Datacenter")
            .description("The local datacenter name for the Cassandra cluster (required by driver 4.x).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private CqlSession cassandraSession;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONTACT_POINTS,
            CLIENT_AUTH,
            CONSISTENCY_LEVEL,
            COMPRESSION_TYPE,
            KEYSPACE,
            USERNAME,
            PASSWORD,
            PROP_SSL_CONTEXT_SERVICE,
            READ_TIMEOUT_MS,
            CONNECT_TIMEOUT_MS,
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
        } catch (RuntimeException e) {
            throw new InitializationException("Failed to connect to Cassandra", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (cassandraSession != null) {
            cassandraSession.close();
            cassandraSession = null;
        }
    }

    @Override
    public CqlSession getCassandraSession() {
        if (cassandraSession != null) {
            return cassandraSession;
        } else {
            throw new ProcessException("Unable to get the Cassandra session.");
        }
    }

    void connectToCassandra(ConfigurationContext context) {
        if (cassandraSession == null) {
            ComponentLog log = getLogger();

            final String contactPointList = context.getProperty(CONTACT_POINTS).getValue();
            final String keyspaceName = context.getProperty(KEYSPACE).getValue();
            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).getValue();
            final CompressionType compressionType = context.getProperty(COMPRESSION_TYPE).asAllowableValue(CompressionType.class);

            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            final SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            final SSLContext sslContext = (sslService != null) ? sslService.createContext() : null;

            final Optional<Integer> readTimeoutMs = Optional.ofNullable(context.getProperty(READ_TIMEOUT_MS))
                    .filter(PropertyValue::isSet)
                    .map(PropertyValue::asInteger);

            final Optional<Integer> connectTimeoutMs = Optional.ofNullable(context.getProperty(CONNECT_TIMEOUT_MS))
                    .filter(PropertyValue::isSet)
                    .map(PropertyValue::asInteger);

            final String localDatacenter = Optional.ofNullable(context.getProperty(LOCAL_DATACENTER))
                    .filter(PropertyValue::isSet)
                    .map(PropertyValue::getValue)
                    .orElse("datacenter1");

            String consistencyLevels = context.getProperty(CONSISTENCY_LEVEL)
                    .evaluateAttributeExpressions().getValue();

            String compressionValue = (compressionType != null)
                    ? compressionType.getValue().toLowerCase()
                    : CompressionType.NONE.getValue().toLowerCase();

            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                            readTimeoutMs.map(Duration::ofMillis).orElse(Duration.ofSeconds(15)))
                    .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT,
                            connectTimeoutMs.map(Duration::ofMillis).orElse(Duration.ofSeconds(15)))
                    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevels)
                    .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compressionValue)
                    .build();

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

            Metadata metadata = cassandraSession.getMetadata();
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
            final int port = (addresses.length > 1) ? Integer.parseInt(addresses[1].trim()) : DEFAULT_CASSANDRA_PORT;

            contactPoints.add(new InetSocketAddress(hostName, port));
        }

        return contactPoints;
    }
}
