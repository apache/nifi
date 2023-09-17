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
package org.apache.nifi.processors.influxdb;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@EventDriven
@SupportsBatching
@Tags({"influxdb", "measurement", "get", "read", "query", "timeseries"})
@CapabilityDescription("Processor to execute InfluxDB query from the content of a FlowFile (preferred) or a scheduled query.  Please check details of the supported queries in InfluxDB documentation (https://www.influxdb.com/).")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractInfluxDBProcessor.INFLUX_DB_ERROR_MESSAGE, description = "InfluxDB error message"),
    @WritesAttribute(attribute = ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY, description = "InfluxDB executed query"),
    })
public class ExecuteInfluxDBQuery extends AbstractInfluxDBProcessor {

    public static final String INFLUX_DB_EXECUTED_QUERY = "influxdb.executed.query";

    private static final int DEFAULT_INFLUX_RESPONSE_CHUNK_SIZE = 0;

    public static final PropertyDescriptor INFLUX_DB_QUERY_RESULT_TIMEUNIT = new PropertyDescriptor.Builder()
            .name("influxdb-query-result-time-unit")
            .displayName("Query Result Time Units")
            .description("The time unit of query results from the InfluxDB")
            .defaultValue(TimeUnit.NANOSECONDS.name())
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues(Arrays.stream(TimeUnit.values()).map( v -> v.name()).collect(Collectors.toSet()))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor INFLUX_DB_QUERY = new PropertyDescriptor.Builder()
            .name("influxdb-query")
            .displayName("InfluxDB Query")
            .description("The InfluxDB query to execute. "
                + "Note: If there are incoming connections, then the query is created from incoming FlowFile's content otherwise"
                + " it is created from this property.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Integer MAX_CHUNK_SIZE = 10000;

    public static final PropertyDescriptor INFLUX_DB_QUERY_CHUNK_SIZE = new PropertyDescriptor.Builder()
            .name("influxdb-query-chunk-size")
            .displayName("Results chunk size")
            .description("Chunking can be used to return results in a stream of smaller batches "
                + "(each has a partial results up to a chunk size) rather than as a single response. "
                + "Chunking queries can return an unlimited number of rows. Note: Chunking is enable when result chunk size is greater than 0")
            .defaultValue(String.valueOf(DEFAULT_INFLUX_RESPONSE_CHUNK_SIZE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createLongValidator(0, MAX_CHUNK_SIZE, true))
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful InfluxDB queries are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Falied InfluxDB queries are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("Failed queries that are retryable exception are routed to this relationship").build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;
    protected Gson gson = new Gson();

    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        tempRelationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(tempRelationships);
        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(DB_NAME);
        tempDescriptors.add(INFLUX_DB_URL);
        tempDescriptors.add(INFLUX_DB_CONNECTION_TIMEOUT);
        tempDescriptors.add(INFLUX_DB_QUERY_RESULT_TIMEUNIT);
        tempDescriptors.add(INFLUX_DB_QUERY);
        tempDescriptors.add(INFLUX_DB_QUERY_CHUNK_SIZE);
        tempDescriptors.add(USERNAME);
        tempDescriptors.add(PASSWORD);
        tempDescriptors.add(CHARSET);
        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        // Either input connection or scheduled query is required
        if ( ! context.getProperty(INFLUX_DB_QUERY).isSet()
           && ! context.hasIncomingConnection() ) {
            String error = "The InfluxDB Query processor requires input connection or scheduled InfluxDB query";
            getLogger().error(error);
            throw new ProcessException(error);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        String query = null;
        String database = null;
        TimeUnit queryResultTimeunit = null;
        Charset charset = null;
        FlowFile outgoingFlowFile = null;

        // If there are incoming connections, prepare query params from flow file
        if ( context.hasIncomingConnection() ) {
            FlowFile incomingFlowFile = session.get();

            if ( incomingFlowFile == null && context.hasNonLoopConnection() ) {
                return;
            }

            charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(incomingFlowFile).getValue());
            if ( incomingFlowFile.getSize() == 0 ) {
                if ( context.getProperty(INFLUX_DB_QUERY).isSet() ) {
                    query = context.getProperty(INFLUX_DB_QUERY).evaluateAttributeExpressions(incomingFlowFile).getValue();
                } else {
                    String message = "FlowFile query is empty and no scheduled query is set";
                    getLogger().error(message);
                    incomingFlowFile = session.putAttribute(incomingFlowFile, INFLUX_DB_ERROR_MESSAGE, message);
                    session.transfer(incomingFlowFile, REL_FAILURE);
                    return;
                }
            } else {

                try {
                    query = getQuery(session, charset, incomingFlowFile);
                } catch(IOException ioe) {
                    getLogger().error("Exception while reading from FlowFile " + ioe.getLocalizedMessage(), ioe);
                    throw new ProcessException(ioe);
                }
            }
            outgoingFlowFile = incomingFlowFile;

        } else {
            outgoingFlowFile = session.create();
            charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(outgoingFlowFile).getValue());
            query = context.getProperty(INFLUX_DB_QUERY).evaluateAttributeExpressions(outgoingFlowFile).getValue();
        }

        database = context.getProperty(DB_NAME).evaluateAttributeExpressions(outgoingFlowFile).getValue();
        queryResultTimeunit = TimeUnit.valueOf(context.getProperty(INFLUX_DB_QUERY_RESULT_TIMEUNIT).evaluateAttributeExpressions(outgoingFlowFile).getValue());

        try {
            long startTimeMillis = System.currentTimeMillis();
            int chunkSize = context.getProperty(INFLUX_DB_QUERY_CHUNK_SIZE).evaluateAttributeExpressions(outgoingFlowFile).asInteger();
            List<QueryResult> result = executeQuery(context, database, query, queryResultTimeunit, chunkSize);

            String json = result.size() == 1 ? gson.toJson(result.get(0)) : gson.toJson(result);

            if ( getLogger().isDebugEnabled() ) {
                getLogger().debug("Query result {} ", new Object[] {result});
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes(charset));
            session.importFrom(bais, outgoingFlowFile);
            bais.close();

            final long endTimeMillis = System.currentTimeMillis();

            if ( ! hasErrors(result) ) {
                outgoingFlowFile = session.putAttribute(outgoingFlowFile, INFLUX_DB_EXECUTED_QUERY, String.valueOf(query));
                session.getProvenanceReporter().send(outgoingFlowFile, makeProvenanceUrl(context, database),
                        (endTimeMillis - startTimeMillis));
                session.transfer(outgoingFlowFile, REL_SUCCESS);
            } else {
                outgoingFlowFile = populateErrorAttributes(session, outgoingFlowFile, query, queryErrors(result));
                session.transfer(outgoingFlowFile, REL_FAILURE);
            }

        } catch (Exception exception) {
            outgoingFlowFile = populateErrorAttributes(session, outgoingFlowFile, query, exception.getMessage());
            if ( exception.getCause() instanceof SocketTimeoutException ) {
                getLogger().error("Failed to read from InfluxDB due SocketTimeoutException to {} and retrying",
                        exception.getCause().getLocalizedMessage(), exception.getCause());
                session.transfer(outgoingFlowFile, REL_RETRY);
            } else {
                getLogger().error("Failed to read from InfluxDB due to {}",
                        exception.getLocalizedMessage(), exception);
                session.transfer(outgoingFlowFile, REL_FAILURE);
            }
            context.yield();
        }
    }

    protected String getQuery(final ProcessSession session, Charset charset, FlowFile incomingFlowFile)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(incomingFlowFile, baos);
        baos.close();
        return new String(baos.toByteArray(), charset);
    }

    protected String makeProvenanceUrl(final ProcessContext context, String database) {
        return new StringBuilder("influxdb://")
            .append(context.getProperty(INFLUX_DB_URL).evaluateAttributeExpressions().getValue()).append("/")
            .append(database).toString();
    }

    protected List<QueryResult> executeQuery(final ProcessContext context, String database, String query, TimeUnit timeunit,
                                             int chunkSize) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        InfluxDB influx = getInfluxDB(context);
        Query influxQuery = new Query(query, database);

        if (chunkSize > 0) {
            List<QueryResult> results = new LinkedList<>();
            influx.query(influxQuery, chunkSize, result -> {
                if (isQueryDone(result.getError())) {
                    latch.countDown();
                } else {
                    results.add(result);
                }
            });
            latch.await();

            return results;
        } else {
            return Collections.singletonList(influx.query(influxQuery, timeunit));
        }
    }

    private boolean isQueryDone(String error) {
        return error != null && error.equals("DONE");
    }

    protected FlowFile populateErrorAttributes(final ProcessSession session, FlowFile flowFile, String query,
            String message) {
        Map<String,String> attributes = new HashMap<>();
        attributes.put(INFLUX_DB_ERROR_MESSAGE, String.valueOf(message));
        attributes.put(INFLUX_DB_EXECUTED_QUERY, String.valueOf(query));
        flowFile = session.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    private Boolean hasErrors(List<QueryResult> results) {
        for (QueryResult result: results) {
            if (result.hasError()) {
                return true;
            }
        }
        return false;
    }

    private String queryErrors(List<QueryResult> results) {
        return results.stream()
                .filter(QueryResult::hasError)
                .map(QueryResult::getError)
                .collect(Collectors.joining("\n"));
    }

    @OnStopped
    public void close() {
        super.close();
    }
}