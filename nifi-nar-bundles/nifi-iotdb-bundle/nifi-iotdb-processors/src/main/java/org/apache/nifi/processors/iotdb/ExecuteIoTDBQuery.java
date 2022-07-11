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
package org.apache.nifi.processors.iotdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@EventDriven
@SupportsBatching
@Tags({"iotdb", "measurement", "get", "read", "query", "timeseries"})
@CapabilityDescription("Processor to execute IoTDB query from the content of a FlowFile (preferred) or a scheduled query.  Please check details of the supported queries in IoTDB documentation (https://iotdb.apache.org/).")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractIoTDBProcessor.IOTDB_ERROR_MESSAGE, description = "IoTDB error message"),
    @WritesAttribute(attribute = ExecuteIoTDBQuery.IOTDB_EXECUTED_QUERY, description = "IoTDB executed query"),
    })
public class ExecuteIoTDBQuery extends AbstractIoTDBProcessor {

    public static final String IOTDB_EXECUTED_QUERY = "iotdb.executed.query";

    private static final int DEFAULT_IOTDB_FETCH_SIZE = 10000;


    public static final PropertyDescriptor IOTDB_QUERY = new PropertyDescriptor.Builder()
            .name("iotdb-query")
            .displayName("IoTDB Query")
            .description("The IoTDB query to execute. "
                + "Note: If there are incoming connections, then the query is created from incoming FlowFile's content otherwise"
                + " it is created from this property.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Integer FETCH_SIZE = 100000;

    public static final PropertyDescriptor IOTDB_QUERY_FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("iotdb-query-chunk-size")
            .displayName("Fetch Size")
            .description("Chunking can be used to return results in a stream of smaller batches "
                + "(each has a partial results up to a chunk size) rather than as a single response. "
                + "Chunking queries can return an unlimited number of rows. Note: Chunking is enable when result chunk size is greater than 0")
            .defaultValue(String.valueOf(DEFAULT_IOTDB_FETCH_SIZE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createLongValidator(0, FETCH_SIZE, true))
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful IoTDB queries are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Falied IoTDB queries are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("Failed queries that are retryable exception are routed to this relationship").build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;
    protected Gson gson = new Gson();

    static {

        relationships = Set.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);
        propertyDescriptors = List
            .of(IOTDB_JDBC_URL, IOTDB_QUERY, IOTDB_QUERY_FETCH_SIZE, USERNAME, PASSWORD, CHARSET);
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
        if ( ! context.getProperty(IOTDB_QUERY).isSet()
           && ! context.hasIncomingConnection() ) {
            String error = "The IoTDB Query processor requires input connection or scheduled IoTDB query";
            getLogger().error(error);
            throw new ProcessException(error);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        String query;
        Charset charset;
        FlowFile outgoingFlowFile;
        int fetchSize=10000;
        // If there are incoming connections, prepare query params from flow file
        if ( context.hasIncomingConnection() ) {
            FlowFile incomingFlowFile = session.get();

            if ( incomingFlowFile == null && context.hasNonLoopConnection() ) {
                return;
            }
            fetchSize = context.getProperty(IOTDB_QUERY_FETCH_SIZE).evaluateAttributeExpressions(incomingFlowFile).asInteger();
            charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(incomingFlowFile).getValue());
            if ( (incomingFlowFile != null ? incomingFlowFile.getSize() : 0) == 0 ) {
                if ( context.getProperty(IOTDB_QUERY).isSet() ) {
                    query = context.getProperty(IOTDB_QUERY).evaluateAttributeExpressions(incomingFlowFile).getValue();
                } else {
                    String message = "FlowFile query is empty and no scheduled query is set";
                    getLogger().error(message);
                    incomingFlowFile = session.putAttribute(incomingFlowFile, IOTDB_ERROR_MESSAGE, message);
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
            query = context.getProperty(IOTDB_QUERY).evaluateAttributeExpressions(outgoingFlowFile).getValue();
        }

        try {
            List<Map<String,Object>> result=executeQuery(fetchSize,context,query);
            String json = result.size() == 1 ? gson.toJson(result.get(0)) : gson.toJson(result);
            if ( getLogger().isDebugEnabled() ) {
                getLogger().debug("Query result {} ", result);
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes(charset));
            outgoingFlowFile= session.importFrom(bais, outgoingFlowFile);
            bais.close();
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, IOTDB_EXECUTED_QUERY, String.valueOf(query));
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, CoreAttributes.MIME_TYPE.key(), "application/json" );
            session.transfer(outgoingFlowFile, REL_SUCCESS);
        } catch (Exception exception) {
            outgoingFlowFile = populateErrorAttributes(session, outgoingFlowFile, query, exception.getMessage());
                getLogger().error("Failed to read from IoTDB due to {}",
                        new Object[]{exception.getLocalizedMessage()}, exception);
                session.transfer(outgoingFlowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected String getQuery(final ProcessSession session, Charset charset, FlowFile incomingFlowFile)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(incomingFlowFile, baos);
        baos.close();
        return baos.toString(charset);
    }

    protected List<Map<String,Object>>  executeQuery(int fetchSize,final ProcessContext context, String query) throws SQLException {
        Connection connection=getIoTDBconnection(context);
        Statement statement =connection.createStatement();
        statement.setFetchSize(fetchSize);
        ResultSet rs=statement.executeQuery(query);
        ResultSetMetaData rsMetaData=rs.getMetaData();
        List<Map<String,Object>> list=new ArrayList<>();
        int count = rsMetaData.getColumnCount();
        while (rs.next()) {
            HashMap<String,Object> resultMap= new HashMap<>();
            for(int i = 1; i<=count; i++) {
                resultMap.put(rsMetaData.getColumnName(i),rs.getObject(i));
            }
            list.add(resultMap);
        }
        return list;

    }

    protected FlowFile populateErrorAttributes(final ProcessSession session, FlowFile flowFile, String query,
            String message) {
        Map<String,String> attributes = new HashMap<>();
        attributes.put(IOTDB_ERROR_MESSAGE, String.valueOf(message));
        attributes.put(IOTDB_EXECUTED_QUERY, String.valueOf(query));
        flowFile = session.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    @OnStopped
    public void close() {
        super.close();
    }
}