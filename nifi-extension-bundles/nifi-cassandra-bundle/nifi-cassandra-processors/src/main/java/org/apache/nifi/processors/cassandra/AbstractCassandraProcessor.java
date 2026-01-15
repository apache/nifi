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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import io.netty.handler.ssl.ClientAuth;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.cassandra.CassandraSessionProviderService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.ssl.SSLContextService;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;

/**
 * AbstractCassandraProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCassandraProcessor extends AbstractProcessor {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    // Common descriptors
    static final PropertyDescriptor CONNECTION_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("cassandra-connection-provider")
            .displayName("Cassandra Connection Provider")
            .description("Specifies the Cassandra connection providing controller service to be used to connect to Cassandra cluster.")
            .required(false)
            .identifiesControllerService(CassandraSessionProviderService.class)
            .build();

    static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("Cassandra Contact Points")
            .description("Contact points are addresses of Cassandra nodes. The list of contact points should be "
                    + "comma-separated and in hostname:port format. Example node1:port,node2:port,...."
                    + " The default client port for Cassandra is 9042, but the port(s) must be explicitly specified.")
            .required(false)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("Keyspace")
            .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to " +
                    "include the keyspace name before any table reference, in case of 'query' native processors or " +
                    "if the processor exposes the 'Table' property, the keyspace name has to be provided with the " +
                    "table name in the form of <KEYSPACE>.<TABLE>")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are NONE, OPTIONAL, REQUIRE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue("REQUIRE")
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consistency Level")
            .description("The strategy for how many replicas must respond before results are returned.")
            .required(false)
            .allowableValues("ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL",
                    "LOCAL_ONE", "LOCAL_QUORUM", "EACH_QUORUM", "SERIAL", "LOCAL_SERIAL")
            .defaultValue("ONE")
            .build();

    static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Enable compression at transport-level requests and responses")
            .required(false)
            .allowableValues("NONE", "LZ4", "SNAPPY")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NONE")
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    static final PropertyDescriptor LOCAL_DATACENTER = new PropertyDescriptor.Builder()
            .name("Local Datacenter")
            .description("The local datacenter name for the Cassandra cluster (required by driver 4.x).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
                    + "it again may succeed.")
            .build();

    protected static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(CONNECTION_PROVIDER_SERVICE);
        descriptors.add(CONTACT_POINTS);
        descriptors.add(KEYSPACE);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(CLIENT_AUTH);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CONSISTENCY_LEVEL);
        descriptors.add(COMPRESSION_TYPE);
        descriptors.add(CHARSET);
    }

    protected final AtomicReference<CqlSession> cassandraSession = new AtomicReference<>(null);

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Ensure that if username or password is set, then the other is too
        String userName = validationContext.getProperty(USERNAME).getValue();
        String password = validationContext.getProperty(PASSWORD).getValue();

        if (StringUtils.isEmpty(userName) != StringUtils.isEmpty(password)) {
            results.add(new ValidationResult.Builder().subject("Username / Password configuration").valid(false).explanation(
                    "If username or password is specified, then the other must be specified as well").build());
        }

        // Ensure that both Connection provider service and the processor specific configurations are not provided
        boolean connectionProviderIsSet = validationContext.getProperty(CONNECTION_PROVIDER_SERVICE).isSet();
        boolean contactPointsIsSet = validationContext.getProperty(CONTACT_POINTS).isSet();

        if (connectionProviderIsSet && contactPointsIsSet) {
            results.add(new ValidationResult.Builder().subject("Cassandra configuration").valid(false).explanation("both " + CONNECTION_PROVIDER_SERVICE.getDisplayName() +
                    " and processor level Cassandra configuration cannot be provided at the same time.").build());
        }

        if (!connectionProviderIsSet && !contactPointsIsSet) {
            results.add(new ValidationResult.Builder().subject("Cassandra configuration").valid(false).explanation("either " + CONNECTION_PROVIDER_SERVICE.getDisplayName() +
                    " or processor level Cassandra configuration has to be provided.").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final boolean connectionProviderIsSet = context.getProperty(CONNECTION_PROVIDER_SERVICE).isSet();

        if (connectionProviderIsSet) {
            CassandraSessionProviderService sessionProvider = context.getProperty(CONNECTION_PROVIDER_SERVICE).asControllerService(CassandraSessionProviderService.class);
            cassandraSession.set(sessionProvider.getCassandraSession());
            return;
        }

        try {
            connectToCassandra(context);
        } catch (AllNodesFailedException ce) {
            getLogger().error("No host in the Cassandra cluster can be contacted successfully to execute this statement.", ce);
            throw new ProcessException(ce);
        } catch (AuthenticationException ae) {
            getLogger().error("Invalid username/password combination", ae);
            throw new ProcessException(ae);
        }
    }

    void connectToCassandra(ProcessContext context) {
        if (cassandraSession.get() == null) {
            ComponentLog log = getLogger();
            final String contactPointList = context.getProperty(CONTACT_POINTS).getValue();
            final String keyspace = context.getProperty(KEYSPACE).getValue();
            final String username = context.getProperty(USERNAME).getValue();
            final String password = context.getProperty(PASSWORD).getValue();
            final String compressionType = context.getProperty(COMPRESSION_TYPE).getValue();

            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            final SSLContextService sslService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            final SSLContext sslContext = (sslService != null) ? sslService.createContext() : null;

            final String localDatacenter = Optional.ofNullable(context.getProperty(LOCAL_DATACENTER))
                    .filter(PropertyValue::isSet)
                    .map(PropertyValue::getValue)
                    .orElse("datacenter1");

            ProgrammaticDriverConfigLoaderBuilder configBuilder = DriverConfigLoader.programmaticBuilder();

            if (compressionType != null && !compressionType.trim().isEmpty()) {
                configBuilder.withString(
                        DefaultDriverOption.PROTOCOL_COMPRESSION,
                        compressionType.trim().toLowerCase()
                );
            }

            CqlSessionBuilder builder = CqlSession.builder()
                    .addContactPoints(contactPoints)
                    .withConfigLoader(configBuilder.build());

            if (localDatacenter != null && !localDatacenter.isEmpty()) {
                builder = builder.withLocalDatacenter(localDatacenter);
            }

            if (keyspace != null && !keyspace.isEmpty()) {
                builder = builder.withKeyspace(keyspace);
            }

            if (username != null && password != null) {
                builder = builder.withAuthCredentials(username, password);
            }

            if (sslContext != null) {
                builder = builder.withSslContext(sslContext);
            }

            CqlSession session = builder.build();
            cassandraSession.set(session);

            log.info("Connected to Cassandra cluster: {}", new Object[]{session.getMetadata().getClusterName()});
        }
    }

    public void stop(ProcessContext context) {
        // Only close the session if not using a centralized connection provider
        if (!context.getProperty(CONNECTION_PROVIDER_SERVICE).isSet()) {
            CqlSession session = cassandraSession.get();
            if (session != null) {
                session.close();
                cassandraSession.set(null);
            }
        }
    }

    protected static Object getCassandraObject(Row row, int i) {
        DataType dataType = row.getColumnDefinitions().get(i).getType();

        try {
            if (dataType.equals(DataTypes.BLOB)) {
                return row.getByteBuffer(i);

            } else if (dataType.equals(DataTypes.VARINT) || dataType.equals(DataTypes.DECIMAL)) {
                Object obj = row.getObject(i);
                return obj != null ? obj.toString() : null;

            } else if (dataType.equals(DataTypes.BOOLEAN)) {
                return row.getBoolean(i);

            } else if (dataType.equals(DataTypes.INT)) {
                return row.getInt(i);

            } else if (dataType.equals(DataTypes.BIGINT) || dataType.equals(DataTypes.COUNTER)) {
                return row.getLong(i);

            } else if (dataType.equals(DataTypes.ASCII) || dataType.equals(DataTypes.TEXT)) {
                return row.getString(i);

            } else if (dataType.equals(DataTypes.FLOAT)) {
                return row.getFloat(i);

            } else if (dataType.equals(DataTypes.DOUBLE)) {
                return row.getDouble(i);

            } else if (dataType.equals(DataTypes.TIMESTAMP)) {
                Instant instant = row.getInstant(i);
                return instant != null ? instant.toString() : null;
            } else if (dataType.equals(DataTypes.DATE)) {
                LocalDate localDate = row.getLocalDate(i);
                return localDate != null ? localDate.toString() : null;
            } else if (dataType.equals(DataTypes.TIME)) {
                LocalTime time = row.getLocalTime(i);
                return time != null ? time.toString() : null;
            } else if (dataType instanceof ListType) {
                ListType listType = (ListType) dataType;
                Class<?> elementClass = getJavaClassForCassandraType(listType.getElementType());
                return row.getList(i, elementClass);

            } else if (dataType instanceof SetType) {
                SetType setType = (SetType) dataType;
                Class<?> elementClass = getJavaClassForCassandraType(setType.getElementType());
                return row.getSet(i, elementClass);

            } else if (dataType instanceof MapType) {
                MapType mapType = (MapType) dataType;
                Class<?> keyClass = getJavaClassForCassandraType(mapType.getKeyType());
                Class<?> valueClass = getJavaClassForCassandraType(mapType.getValueType());
                return row.getMap(i, keyClass, valueClass);
            }

            // Fallback for any other types
            Object value = row.getObject(i);
            return value != null ? value.toString() : null;

        } catch (Exception e) {
            return row.getObject(i) != null ? row.getObject(i).toString() : null;
        }
    }

    static Class<?> getJavaClassForCassandraType(DataType type) {
        if (type.equals(DataTypes.ASCII) || type.equals(DataTypes.TEXT)) {
            return String.class;
        }
        if (type.equals(DataTypes.INT)) {
            return Integer.class;
        }
        if (type.equals(DataTypes.BIGINT) || type.equals(DataTypes.COUNTER)) {
            return Long.class;
        }
        if (type.equals(DataTypes.BOOLEAN)) {
            return Boolean.class;
        }
        if (type.equals(DataTypes.FLOAT)) {
            return Float.class;
        }
        if (type.equals(DataTypes.DOUBLE)) {
            return Double.class;
        }
        if (type.equals(DataTypes.VARINT) || type.equals(DataTypes.DECIMAL)) {
            return String.class;
        }
        if (type.equals(DataTypes.UUID) || type.equals(DataTypes.TIMEUUID)) {
            return java.util.UUID.class;
        }
        if (type.equals(DataTypes.TIMESTAMP)) {
            return java.time.Instant.class;
        }
        if (type.equals(DataTypes.DATE)) {
            return java.time.LocalDate.class;
        }
        if (type.equals(DataTypes.TIME)) {
            return Long.class;
        }
        if (type.equals(DataTypes.BLOB)) {
            return java.nio.ByteBuffer.class;
        }
        return Object.class;
    }

    /**
     * This method will create a schema a union field consisting of null and the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getUnionFieldType(String dataType) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(getSchemaForType(dataType)).endUnion();
    }

    /**
     * This method will create an Avro schema for the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getSchemaForType(String dataType) {
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
            default: throw new IllegalArgumentException(String.format("Unknown Avro primitive type: %s", dataType));
        }
        return returnSchema;
    }

    protected static String getPrimitiveAvroTypeFromCassandraType(DataType dataType) {

        if (dataType.equals(DataTypes.ASCII)
                || dataType.equals(DataTypes.TEXT)
                || dataType.equals(DataTypes.TIMESTAMP)
                || dataType.equals(DataTypes.DATE)
                || dataType.equals(DataTypes.TIME)
                || dataType.equals(DataTypes.TIMEUUID)
                || dataType.equals(DataTypes.UUID)
                || dataType.equals(DataTypes.INET)
                || dataType.equals(DataTypes.VARINT)
                || dataType.equals(DataTypes.DECIMAL)
                || dataType.equals(DataTypes.DURATION)) {

            return "string";

        } else if (dataType.equals(DataTypes.BOOLEAN)) {
            return "boolean";

        } else if (dataType.equals(DataTypes.INT)
                || dataType.equals(DataTypes.SMALLINT)
                || dataType.equals(DataTypes.TINYINT)) {
            return "int";

        } else if (dataType.equals(DataTypes.BIGINT)
                || dataType.equals(DataTypes.COUNTER)) {
            return "long";

        } else if (dataType.equals(DataTypes.FLOAT)) {
            return "float";

        } else if (dataType.equals(DataTypes.DOUBLE)) {
            return "double";

        } else if (dataType.equals(DataTypes.BLOB)) {
            return "bytes";

        } else {
            throw new IllegalArgumentException(
                    String.format("createSchema: Unknown Cassandra data type %s cannot be converted to Avro type", dataType)
            );
        }
    }

    protected static DataType getPrimitiveDataTypeFromString(String dataTypeName) {
        List<DataType> primitiveTypes = List.of(
                DataTypes.ASCII,
                DataTypes.BIGINT,
                DataTypes.BLOB,
                DataTypes.BOOLEAN,
                DataTypes.COUNTER,
                DataTypes.DATE,
                DataTypes.DECIMAL,
                DataTypes.DOUBLE,
                DataTypes.FLOAT,
                DataTypes.INET,
                DataTypes.INT,
                DataTypes.SMALLINT,
                DataTypes.TEXT,
                DataTypes.TIME,
                DataTypes.TIMESTAMP,
                DataTypes.TIMEUUID,
                DataTypes.TINYINT,
                DataTypes.UUID,
                DataTypes.VARINT
        );

        for (DataType primitiveType : primitiveTypes) {
            if (primitiveType.asCql(false, false).equalsIgnoreCase(dataTypeName)) {
                return primitiveType;
            }
        }
        return null;
    }

    protected Object convertToCassandraWriteValue(
            Object value,
            DataType cassandraType,
            String fieldName) {

        try {
            if (value == null) {
                return null;
            }

            if (cassandraType.equals(DataTypes.BLOB)) {
                return value;
            } else if (cassandraType.equals(DataTypes.VARINT)
                    || cassandraType.equals(DataTypes.DECIMAL)) {
                return DataTypeUtils.toBigDecimal(value, fieldName);
            } else if (cassandraType.equals(DataTypes.BOOLEAN)) {
                return DataTypeUtils.toBoolean(value, fieldName);
            } else if (cassandraType.equals(DataTypes.INT)) {
                return ((Number) value).intValue();
            } else if (cassandraType.equals(DataTypes.BIGINT)
                    || cassandraType.equals(DataTypes.COUNTER)) {
                return ((Number) value).longValue();
            } else if (cassandraType.equals(DataTypes.ASCII)
                    || cassandraType.equals(DataTypes.TEXT)) {
                return value.toString();
            } else if (cassandraType.equals(DataTypes.FLOAT)) {
                return ((Number) value).floatValue();
            } else if (cassandraType.equals(DataTypes.DOUBLE)) {
                return ((Number) value).doubleValue();
            } else if (cassandraType.equals(DataTypes.UUID)
                    || cassandraType.equals(DataTypes.TIMEUUID)) {
                return DataTypeUtils.toUUID(value);
            } else if (cassandraType.equals(DataTypes.TIMESTAMP)) {
                if (value instanceof Instant) {
                    return value;
                } else if (value instanceof Number) {
                    return Instant.ofEpochMilli(((Number) value).longValue());
                }
                return Instant.parse(value.toString());
            } else if (cassandraType.equals(DataTypes.DATE)) {
                if (value instanceof LocalDate) {
                    return value;
                } else if (value instanceof Number) {
                    return LocalDate.ofEpochDay(((Number) value).longValue());
                }
                return LocalDate.parse(value.toString());
            } else if (cassandraType.equals(DataTypes.TIME)) {
                if (value instanceof LocalTime) {
                    return value;
                } else if (value instanceof Number) {
                    return LocalTime.ofNanoOfDay(((Number) value).longValue());
                }
                return LocalTime.parse(value.toString());
            } else if (cassandraType instanceof ListType) {
                if (value instanceof List) {
                    return value;
                } else if (value.getClass().isArray()) {
                    return Arrays.asList((Object[]) value);
                }
                throw new IllegalArgumentException(
                        "Expected List or array for Cassandra list but got " + value.getClass());
            } else if (cassandraType instanceof SetType) {
                if (value instanceof Set) {
                    return value;
                } else if (value instanceof List) {
                    return new HashSet<>((List<?>) value);
                } else if (value.getClass().isArray()) {
                    return new HashSet<>(Arrays.asList((Object[]) value));
                }
                throw new IllegalArgumentException(
                        "Expected Set, List or array for Cassandra set but got " + value.getClass());
            } else if (cassandraType instanceof MapType) {
                if (value instanceof Map) {
                    return value;
                }
                throw new IllegalArgumentException(
                        "Expected Map for Cassandra map but got " + value.getClass());
            }

            return value.toString();

        } catch (Exception e) {
            throw new ProcessException(
                    "Failed to convert field '" + fieldName +
                            "' to Cassandra type " + cassandraType, e);
        }
    }

    /**
     * Gets a list of InetSocketAddress objects that correspond to host:port entries for Cassandra contact points
     *
     * @param contactPointList A comma-separated list of Cassandra contact points (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the Cassandra contact points
     */
    protected List<InetSocketAddress> getContactPoints(String contactPointList) {

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
