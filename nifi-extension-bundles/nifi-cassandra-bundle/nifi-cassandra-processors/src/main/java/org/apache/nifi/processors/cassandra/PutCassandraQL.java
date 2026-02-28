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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


@SupportsBatching
@Tags({"cassandra", "cql", "put", "insert", "update", "set"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Execute provided Cassandra Query Language (CQL) statement on a Cassandra 1.x, 2.x, or 3.0.x cluster. "
        + "The content of an incoming FlowFile is expected to be the CQL command to execute. The CQL command may use "
        + "the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes with the "
        + "naming convention cql.args.N.type and cql.args.N.value, where N is a positive integer. The cql.args.N.type "
        + "is expected to be a lowercase string indicating the Cassandra type.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "cql.args.N.type",
                description = "Incoming FlowFiles are expected to be parameterized CQL statements. The type of each "
                        + "parameter is specified as a lowercase string corresponding to the Cassandra data type (text, "
                        + "int, boolean, e.g.). In the case of collections, the primitive type(s) of the elements in the "
                        + "collection should be comma-delimited, follow the collection type, and be enclosed in angle brackets "
                        + "(< and >), for example set<text> or map<timestamp, int>."),
        @ReadsAttribute(attribute = "cql.args.N.value",
                description = "Incoming FlowFiles are expected to be parameterized CQL statements. The value of the "
                        + "parameters are specified as cql.args.1.value, cql.args.2.value, cql.args.3.value, and so on. The "
                        + " type of the cql.args.1.value parameter is specified by the cql.args.1.type attribute.")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutCassandraQL extends AbstractCassandraProcessor {

    public static final PropertyDescriptor STATEMENT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running CQL select query. Must be of format "
                    + "<duration> <TimeUnit> where <duration> is a non-negative integer and TimeUnit is a supported "
                    + "Time Unit, such as: nanos, millis, secs, mins, hrs, days. A value of zero means there is no limit. ")
            .defaultValue("0 seconds")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor STATEMENT_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Statement Cache Size")
            .description("The maximum number of CQL Prepared Statements to cache. This can improve performance if many incoming flow files have the same CQL statement "
                    + "with different values for the parameters. If this property is set to zero, the cache is effectively disabled.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private static final Pattern CQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("cql\\.args\\.(\\d+)\\.type");

    // Matches on top-level type (primitive types like text,int) and also for collections (like list<boolean> and map<float,double>)
    private static final Pattern CQL_TYPE_PATTERN = Pattern.compile("([^<]+)(<([^,>]+)(,([^,>]+))*>)?");

    /**
     * LRU cache for the compiled patterns. The size of the cache is determined by the value of the Statement Cache Size property
     */
    @VisibleForTesting
    ConcurrentMap<String, PreparedStatement> statementCache;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            Stream.concat(
                    COMMON_PROPERTY_DESCRIPTORS.stream(),
                    Stream.of(
                            STATEMENT_TIMEOUT,
                            STATEMENT_CACHE_SIZE
                    )
            ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_RETRY
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        final int statementCacheSize = context.getProperty(STATEMENT_CACHE_SIZE).asInteger();
        statementCache = new ConcurrentHashMap<>(Math.max(statementCacheSize, 1));
        getLogger().debug("Prepared statement cache initialized with size {}", statementCacheSize);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final long statementTimeout = context.getProperty(STATEMENT_TIMEOUT).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        final CqlSession connectionSession = cassandraSession.get();

        String cql = getCQL(session, flowFile, charset);
        try {
            PreparedStatement statement = statementCache.get(cql);
            if (statement == null) {
                statement = connectionSession.prepare(cql);
                statementCache.put(cql, statement);
            }

            BoundStatementBuilder boundBuilder = statement.boundStatementBuilder();
            Map<String, String> attributes = flowFile.getAttributes();

            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                String key = entry.getKey();
                Matcher matcher = CQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
                if (matcher.matches()) {
                    int paramIndex = Integer.parseInt(matcher.group(1));
                    String paramType = entry.getValue();
                    if (StringUtils.isBlank(paramType)) {
                        throw new ProcessException(String.format("Value of %s is empty; must contain a valid type", key));
                    }

                    paramType = paramType.trim();
                    String valueAttrName = "cql.args." + paramIndex + ".value";
                    String paramValue = attributes.get(valueAttrName);

                    try {
                        setStatementObject(boundBuilder, paramIndex - 1, valueAttrName, paramValue, paramType.trim());

                    } catch (IllegalArgumentException e) {
                        throw new ProcessException(
                                String.format("The value of the %s is '%s', which cannot be converted into the necessary data type: %s",
                                        valueAttrName, paramValue, paramType), e);
                    }
                }
            }

            BoundStatement boundStatement = boundBuilder.build();

            try {
                CompletableFuture<AsyncResultSet> future = connectionSession.executeAsync(boundStatement).toCompletableFuture();
                if (statementTimeout > 0) {
                    future.get(statementTimeout, TimeUnit.MILLISECONDS);
                } else {
                    future.get();
                }
                final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                // This isn't a real URI but since Cassandra is distributed we just use the cluster name
                String transitUri = "cassandra://" + connectionSession.getMetadata().getClusterName();
                session.getProvenanceReporter().send(flowFile, transitUri, transmissionMillis, true);
                session.transfer(flowFile, REL_SUCCESS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Thread interrupted while executing CQL", e);
            } catch (ExecutionException e) {
                throw new ProcessException("Error executing CQL statement", e);
            } catch (final TimeoutException e) {
                throw new ProcessException(e);
            }

        } catch (AllNodesFailedException ex) {
            logger.error("All Cassandra nodes failed while executing the query", ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_RETRY);
        } catch (QueryExecutionException qee) {
            logger.error("Cannot execute the statement with the requested consistency level successfully", qee);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_RETRY);
        } catch (QueryValidationException qve) {
            logger.error("The CQL statement is invalid; routing flowFile {} to failure", flowFile);
            logger.debug("Invalid CQL statement: {}", cql, qve);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (ProcessException e) {
            logger.error("Unable to execute CQL statement for flowFile {}", flowFile);
            logger.debug("Failed CQL statement: {}", cql, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
         * Determines the CQL statement that should be executed for the given FlowFile
         *
         * @param session  the session that can be used to access the given FlowFile
         * @param flowFile the FlowFile whose CQL statement should be executed
         * @return the CQL that is associated with the given FlowFile
         */
    private String getCQL(final ProcessSession session, final FlowFile flowFile, final Charset charset) {
        // Read the CQL from the FlowFile's content
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        return new String(buffer, charset);
    }

    /**
         / * Determines how to map the given value to the appropriate Cassandra data type and returns the object as
          * represented by the given type. This can be used in a Prepared/BoundStatement.
          *
          * @param builder  the BoundStatementBuilder for setting objects on
          * @param paramIndex the index of the parameter at which to set the object
          * @param attrName   the name of the attribute that the parameter is coming from - for logging purposes
          * @param paramValue the value of the CQL parameter to set
          * @param paramType  the Cassandra data type of the CQL parameter to set
          * @throws IllegalArgumentException if the PreparedStatement throws a CQLException when calling the appropriate setter
          */
    protected void setStatementObject(BoundStatementBuilder builder, final int paramIndex, final String attrName,
                                      final String paramValue, final String paramType)  throws IllegalArgumentException {
        if (paramValue == null) {
            builder.setToNull(paramIndex);
            return;
        }

        if (paramType == null || paramType.isBlank()) {
            throw new IllegalArgumentException(String.format("Parameter type for %s cannot be null or empty", attrName));
        }

        final Matcher matcher = CQL_TYPE_PATTERN.matcher(paramType);
        if (!matcher.find() || matcher.groupCount() < 1) {
            throw new IllegalArgumentException(String.format("Cannot create object of type %s using input %s", paramType, paramValue));
        }

        final String mainTypeString = matcher.group(1).toLowerCase();
        final DataType mainType = getPrimitiveDataTypeFromString(mainTypeString);

        if (mainType != null) {
            try {
                if (mainType.equals(DataTypes.ASCII)
                        || mainType.equals(DataTypes.TEXT)
                        || mainType.equals(DataTypes.INET)) {

                    builder = builder.set(paramIndex, paramValue, String.class);

                } else if (mainType.equals(DataTypes.BOOLEAN)) {
                    if (!"true".equalsIgnoreCase(paramValue)
                            && !"false".equalsIgnoreCase(paramValue)) {
                        throw new IllegalArgumentException("Invalid boolean value");
                    }
                    builder = builder.set(paramIndex, Boolean.parseBoolean(paramValue), Boolean.class);

                } else if (mainType.equals(DataTypes.INT)) {
                    builder = builder.set(paramIndex, Integer.parseInt(paramValue), Integer.class);

                } else if (mainType.equals(DataTypes.BIGINT)
                        || mainType.equals(DataTypes.COUNTER)) {
                    builder = builder.set(paramIndex, Long.parseLong(paramValue), Long.class);

                } else if (mainType.equals(DataTypes.VARINT)) {
                    builder = builder.set(paramIndex, new BigInteger(paramValue), BigInteger.class);

                } else if (mainType.equals(DataTypes.DECIMAL)) {
                    builder = builder.set(paramIndex, new BigDecimal(paramValue), BigDecimal.class);

                } else if (mainType.equals(DataTypes.FLOAT)) {
                    builder = builder.set(paramIndex, Float.parseFloat(paramValue), Float.class);

                } else if (mainType.equals(DataTypes.DOUBLE)) {
                    builder = builder.set(paramIndex, Double.parseDouble(paramValue), Double.class);

                } else if (mainType.equals(DataTypes.BLOB)) {
                    builder = builder.set(
                            paramIndex,
                            ByteBuffer.wrap(paramValue.getBytes(StandardCharsets.UTF_8)),
                            ByteBuffer.class
                    );

                } else if (mainType.equals(DataTypes.TIMESTAMP)) {
                    builder = builder.set(paramIndex, Instant.parse(paramValue), Instant.class);

                } else if (mainType.equals(DataTypes.DATE)) {
                    builder = builder.set(paramIndex, LocalDate.parse(paramValue), LocalDate.class);

                } else if (mainType.equals(DataTypes.TIME)) {
                    builder = builder.set(paramIndex, java.time.LocalTime.parse(paramValue), LocalTime.class);

                } else if (mainType.equals(DataTypes.UUID)
                        || mainType.equals(DataTypes.TIMEUUID)) {
                    builder = builder.set(paramIndex, UUID.fromString(paramValue), UUID.class);

                } else {
                    throw new IllegalArgumentException(
                            "Unsupported primitive type: " + mainType.asCql(false, false));
                }

            } catch (RuntimeException e) {
                throw new IllegalArgumentException(String.format("Cannot convert value '%s' to type %s", paramValue, paramType), e);
            }
            return;
        }

        if (matcher.groupCount() < 3) {
            throw new IllegalArgumentException(String.format("Collection type %s needs parameterized type(s), such as list<text>",
                    mainTypeString));
        }

        DataType elementType = getPrimitiveDataTypeFromString(matcher.group(3));
        if (elementType == null) {
            throw new IllegalArgumentException("Nested collections are not supported");
        }

        Class<?> elementClass = getJavaClassForCassandraType(elementType);

        if ("map".equalsIgnoreCase(mainTypeString)) {
            if (matcher.groupCount() < 5) {
                throw new IllegalArgumentException("Map requires key and value types");
            }
            DataType valueType = getPrimitiveDataTypeFromString(matcher.group(5));
            if (valueType == null) {
                throw new IllegalArgumentException("Nested collections are not supported in map");
            }
            Class<?> valueClass = getJavaClassForCassandraType(valueType);
            Map<?, ?> mapValue = parseMap(paramValue, elementClass, valueClass);
            builder = builder.set(paramIndex, mapValue, Map.class);
            return;
        }

        if ("set".equalsIgnoreCase(mainTypeString)) {
            Set<?> setValue = parseSet(paramValue, elementClass);
            builder = builder.set(paramIndex, setValue, Set.class);
            return;
        }

        if ("list".equalsIgnoreCase(mainTypeString)) {
            List<?> listValue = parseList(paramValue, elementClass);
            builder = builder.set(paramIndex, listValue, List.class);
            return;
        }

        throw new IllegalArgumentException(String.format("Cannot create object of type %s using input %s",
                paramType, paramValue));
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    protected static Map<?, ?> parseMap(String json, Class<?> keyClass, Class<?> valueClass) {
        try {
            return objectMapper.readValue(json, objectMapper.getTypeFactory().constructMapType(HashMap.class, keyClass, valueClass));
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Cannot parse list from value: %s", json), e);
        }
    }

    protected static Set<?> parseSet(String json, Class<?> elementClass) {
        try {
            return objectMapper.readValue(json, objectMapper.getTypeFactory().constructCollectionType(Set.class, elementClass));
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Cannot parse map from value: %s", json), e);
        }
    }

    protected static List<?> parseList(String json, Class<?> elementClass) {
        try {
            return objectMapper.readValue(json, objectMapper.getTypeFactory().constructCollectionType(List.class, elementClass));
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Cannot parse list from value: %s", json), e);
        }
    }

    @OnStopped
    @Override
    public void stop(final ProcessContext context) {
        super.stop(context);

        if (statementCache != null) {
            statementCache.clear();
        }
    }
}
