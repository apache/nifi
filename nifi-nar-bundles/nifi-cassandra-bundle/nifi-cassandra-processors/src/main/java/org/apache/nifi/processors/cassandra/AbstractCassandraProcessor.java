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
package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCassandraProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCassandraProcessor extends AbstractProcessor {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    // Common descriptors
    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("Contact points are addresses of Cassandra nodes. The list of contact points should be "
                    + "comma-separated and in hostname:port format. Example node1:port,node2:port,...."
                    + " The default client port for Cassandra is 9042, but the port(s) must be explicitly specified.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Keyspace")
            .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to "
                    + "include the keyspace name before any table reference.")
            .required(false)
            .expressionLanguageSupported(true)
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
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(true)
            .allowableValues(ConsistencyLevel.values())
            .defaultValue("ONE")
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(CONTACT_POINTS);
        descriptors.add(KEYSPACE);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(CLIENT_AUTH);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CONSISTENCY_LEVEL);
        descriptors.add(CHARSET);
    }

    protected final AtomicReference<Cluster> cluster = new AtomicReference<>(null);
    protected final AtomicReference<Session> cassandraSession = new AtomicReference<>(null);

    protected static final CodecRegistry codecRegistry = new CodecRegistry();

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Ensure that if username or password is set, then the other is too
        String userName = validationContext.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = validationContext.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        if (StringUtils.isEmpty(userName) != StringUtils.isEmpty(password)) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "If username or password is specified, then the other must be specified as well").build());
        }

        return results;
    }

    protected void connectToCassandra(ProcessContext context) {
        if (cluster.get() == null) {
            ComponentLog log = getLogger();
            final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();
            final String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            // Set up the client for secure (SSL/TLS communications) if configured to do so
            final SSLContextService sslService =
                    context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            final String rawClientAuth = context.getProperty(CLIENT_AUTH).getValue();
            final SSLContext sslContext;

            if (sslService != null) {
                final SSLContextService.ClientAuth clientAuth;
                if (StringUtils.isBlank(rawClientAuth)) {
                    clientAuth = SSLContextService.ClientAuth.REQUIRED;
                } else {
                    try {
                        clientAuth = SSLContextService.ClientAuth.valueOf(rawClientAuth);
                    } catch (final IllegalArgumentException iae) {
                        throw new ProviderCreationException(String.format("Unrecognized client auth '%s'. Possible values are [%s]",
                                rawClientAuth, StringUtils.join(SslContextFactory.ClientAuth.values(), ", ")));
                    }
                }
                sslContext = sslService.createSSLContext(clientAuth);
            } else {
                sslContext = null;
            }

            final String username, password;
            PropertyValue usernameProperty = context.getProperty(USERNAME).evaluateAttributeExpressions();
            PropertyValue passwordProperty = context.getProperty(PASSWORD).evaluateAttributeExpressions();

            if (usernameProperty != null && passwordProperty != null) {
                username = usernameProperty.getValue();
                password = passwordProperty.getValue();
            } else {
                username = null;
                password = null;
            }

            // Create the cluster and connect to it
            Cluster newCluster = createCluster(contactPoints, sslContext, username, password);
            PropertyValue keyspaceProperty = context.getProperty(KEYSPACE).evaluateAttributeExpressions();
            final Session newSession;
            if (keyspaceProperty != null) {
                newSession = newCluster.connect(keyspaceProperty.getValue());
            } else {
                newSession = newCluster.connect();
            }
            newCluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));
            Metadata metadata = newCluster.getMetadata();
            log.info("Connected to Cassandra cluster: {}", new Object[]{metadata.getClusterName()});
            cluster.set(newCluster);
            cassandraSession.set(newSession);
        }
    }

    /**
     * Uses a Cluster.Builder to create a Cassandra cluster reference using the given parameters
     *
     * @param contactPoints The contact points (hostname:port list of Cassandra nodes)
     * @param sslContext    The SSL context (used for secure connections)
     * @param username      The username for connection authentication
     * @param password      The password for connection authentication
     * @return A reference to the Cluster object associated with the given Cassandra configuration
     */
    protected Cluster createCluster(List<InetSocketAddress> contactPoints, SSLContext sslContext,
                                    String username, String password) {
        Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(contactPoints);
        if (sslContext != null) {
            JdkSSLOptions sslOptions = JdkSSLOptions.builder()
                    .withSSLContext(sslContext)
                    .build();
            builder = builder.withSSL(sslOptions);
        }
        if (username != null && password != null) {
            builder = builder.withCredentials(username, password);
        }
        return builder.build();
    }

    public void stop() {
        if (cassandraSession.get() != null) {
            cassandraSession.get().close();
            cassandraSession.set(null);
        }
        if (cluster.get() != null) {
            cluster.get().close();
            cluster.set(null);
        }
    }


    protected static Object getCassandraObject(Row row, int i, DataType dataType) {
        if (dataType.equals(DataType.blob())) {
            return row.getBytes(i);

        } else if (dataType.equals(DataType.varint()) || dataType.equals(DataType.decimal())) {
            // Avro can't handle BigDecimal and BigInteger as numbers - it will throw an
            // AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
            return row.getObject(i).toString();

        } else if (dataType.equals(DataType.cboolean())) {
            return row.getBool(i);

        } else if (dataType.equals(DataType.cint())) {
            return row.getInt(i);

        } else if (dataType.equals(DataType.bigint())
                || dataType.equals(DataType.counter())) {
            return row.getLong(i);

        } else if (dataType.equals(DataType.ascii())
                || dataType.equals(DataType.text())
                || dataType.equals(DataType.varchar())) {
            return row.getString(i);

        } else if (dataType.equals(DataType.cfloat())) {
            return row.getFloat(i);

        } else if (dataType.equals(DataType.cdouble())) {
            return row.getDouble(i);

        } else if (dataType.equals(DataType.timestamp())) {
            return row.getTimestamp(i);

        } else if (dataType.equals(DataType.date())) {
            return row.getDate(i);

        } else if (dataType.equals(DataType.time())) {
            return row.getTime(i);

        } else if (dataType.isCollection()) {

            List<DataType> typeArguments = dataType.getTypeArguments();
            if (typeArguments == null || typeArguments.size() == 0) {
                throw new IllegalArgumentException("Column[" + i + "] " + dataType.getName()
                        + " is a collection but no type arguments were specified!");
            }
            // Get the first type argument, to be used for lists and sets (and the first in a map)
            DataType firstArg = typeArguments.get(0);
            TypeCodec firstCodec = codecRegistry.codecFor(firstArg);
            if (dataType.equals(DataType.set(firstArg))) {
                return row.getSet(i, firstCodec.getJavaType());
            } else if (dataType.equals(DataType.list(firstArg))) {
                return row.getList(i, firstCodec.getJavaType());
            } else {
                // Must be an n-arg collection like map
                DataType secondArg = typeArguments.get(1);
                TypeCodec secondCodec = codecRegistry.codecFor(secondArg);
                if (dataType.equals(DataType.map(firstArg, secondArg))) {
                    return row.getMap(i, firstCodec.getJavaType(), secondCodec.getJavaType());
                }
            }

        } else {
            // The different types that we support are numbers (int, long, double, float),
            // as well as boolean values and Strings. Since Avro doesn't provide
            // timestamp types, we want to convert those to Strings. So we will cast anything other
            // than numbers or booleans to strings by using the toString() method.
            return row.getObject(i).toString();
        }
        return null;
    }

    /**
     * This method will create a schema a union field consisting of null and the specified type.
     *
     * @param dataType The data type of the field
     */
    public static Schema getUnionFieldType(String dataType) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(getSchemaForType(dataType)).endUnion();
    }

    /**
     * This method will create an Avro schema for the specified type.
     *
     * @param dataType The data type of the field
     */
    public static Schema getSchemaForType(String dataType) {
        SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();
        Schema returnSchema;
        switch (dataType) {
            case "string":
                returnSchema = typeBuilder.stringType();
                break;
            case "boolean":
                returnSchema = typeBuilder.booleanType();
                break;
            case "int":
                returnSchema = typeBuilder.intType();
                break;
            case "long":
                returnSchema = typeBuilder.longType();
                break;
            case "float":
                returnSchema = typeBuilder.floatType();
                break;
            case "double":
                returnSchema = typeBuilder.doubleType();
                break;
            case "bytes":
                returnSchema = typeBuilder.bytesType();
                break;
            default:
                throw new IllegalArgumentException("Unknown Avro primitive type: " + dataType);
        }
        return returnSchema;
    }

    public static String getPrimitiveAvroTypeFromCassandraType(DataType dataType) {
        // Map types from Cassandra to Avro where possible
        if (dataType.equals(DataType.ascii())
                || dataType.equals(DataType.text())
                || dataType.equals(DataType.varchar())
                // Nonstandard types represented by this processor as a string
                || dataType.equals(DataType.timestamp())
                || dataType.equals(DataType.timeuuid())
                || dataType.equals(DataType.uuid())
                || dataType.equals(DataType.inet())
                || dataType.equals(DataType.varint())) {
            return "string";

        } else if (dataType.equals(DataType.cboolean())) {
            return "boolean";

        } else if (dataType.equals(DataType.cint())) {
            return "int";

        } else if (dataType.equals(DataType.bigint())
                || dataType.equals(DataType.counter())) {
            return "long";

        } else if (dataType.equals(DataType.cfloat())) {
            return "float";

        } else if (dataType.equals(DataType.cdouble())) {
            return "double";

        } else if (dataType.equals(DataType.blob())) {
            return "bytes";

        } else {
            throw new IllegalArgumentException("createSchema: Unknown Cassandra data type " + dataType.getName()
                    + " cannot be converted to Avro type");
        }
    }

    public static DataType getPrimitiveDataTypeFromString(String dataTypeName) {
        Set<DataType> primitiveTypes = DataType.allPrimitiveTypes();
        for (DataType primitiveType : primitiveTypes) {
            if (primitiveType.toString().equals(dataTypeName)) {
                return primitiveType;
            }
        }
        return null;
    }

    /**
     * Gets a list of InetSocketAddress objects that correspond to host:port entries for Cassandra contact points
     *
     * @param contactPointList A comma-separated list of Cassandra contact points (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the Cassandra contact points
     */
    public List<InetSocketAddress> getContactPoints(String contactPointList) {

        if (contactPointList == null) {
            return null;
        }
        final List<String> contactPointStringList = Arrays.asList(contactPointList.split(","));
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

