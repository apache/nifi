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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import com.google.gson.Gson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"influxdb", "measurement","get", "read", "query", "timeseries"})
@CapabilityDescription("Processor to execute InfluxDB query from the content of a FlowFile.  Please check details of the supported queries in InfluxDB documentation (https://www.influxdb.com/).")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractInfluxDBProcessor.INFLUX_DB_ERROR_MESSAGE, description = "InfluxDB error message"),
    @WritesAttribute(attribute = ExecuteInfluxDBQuery.INFLUX_DB_EXECUTED_QUERY, description = "InfluxDB executed query"),
    })
public class ExecuteInfluxDBQuery extends AbstractInfluxDBProcessor {

    public static final String INFLUX_DB_EXECUTED_QUERY = "influxdb.executed.query";

    public static final PropertyDescriptor INFLUX_DB_QUERY_RESULT_TIMEUNIT = new PropertyDescriptor.Builder()
            .name("influxdb-query-result-time-unit")
            .displayName("Query Result Time Units")
            .description("The time unit of query results from the InfluxDB")
            .defaultValue(TimeUnit.NANOSECONDS.name())
            .required(true)
            .expressionLanguageSupported(true)
            .allowableValues(Arrays.stream(TimeUnit.values()).map( v -> v.name()).collect(Collectors.toSet()))
            .sensitive(false)
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
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        if ( flowFile.getSize() == 0) {
            getLogger().error("Empty query");
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, "Empty query size is " + flowFile.getSize());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        String database = context.getProperty(DB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        TimeUnit queryResultTimeunit =
            TimeUnit.valueOf(context.getProperty(INFLUX_DB_QUERY_RESULT_TIMEUNIT).evaluateAttributeExpressions(flowFile).getValue());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        String query = new String(baos.toByteArray(), charset);

        try {
            long startTimeMillis = System.currentTimeMillis();
            QueryResult result = executeQuery(context, database, query, queryResultTimeunit);
            String json = gson.toJson(result);

            if ( getLogger().isDebugEnabled() ) {
                getLogger().debug("Query result {} ", new Object[] {result});
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes(charset));
            session.importFrom(bais, flowFile);
            final long endTimeMillis = System.currentTimeMillis();

            if ( ! result.hasError() ) {
                flowFile = session.putAttribute(flowFile, INFLUX_DB_EXECUTED_QUERY, String.valueOf(query));
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                flowFile = populateErrorAttributes(session, flowFile, query, result.getError());
                session.transfer(flowFile, REL_FAILURE);
            }

            session.getProvenanceReporter().send(flowFile, new StringBuilder("influxdb://")
                .append(context.getProperty(INFLUX_DB_URL).evaluateAttributeExpressions().getValue()).append("/")
                .append(database).toString(),
                (endTimeMillis - startTimeMillis));
        } catch (Exception exception) {
            flowFile = populateErrorAttributes(session, flowFile, query, exception.getMessage());
            if ( exception.getCause() instanceof SocketTimeoutException ) {
                getLogger().error("Failed to read from influxDB due SocketTimeoutException to {} and retrying",
                        new Object[]{exception.getLocalizedMessage()}, exception);
                session.transfer(flowFile, REL_RETRY);
            } else {
                getLogger().error("Failed to read from influxDB due to {}",
                        new Object[]{exception.getLocalizedMessage()}, exception);
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();
        }
    }

    protected QueryResult executeQuery(final ProcessContext context, String database, String query, TimeUnit timeunit) {
        return getInfluxDB(context).query(new Query(query, database),timeunit);
    }

    protected FlowFile populateErrorAttributes(final ProcessSession session, FlowFile flowFile, String query,
            String message) {
        Map<String,String> attributes = new HashMap<>();
        attributes.put(INFLUX_DB_ERROR_MESSAGE, String.valueOf(message));
        attributes.put(INFLUX_DB_EXECUTED_QUERY, String.valueOf(query));
        flowFile = session.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    @OnStopped
    public void close() {
        super.close();
    }
}