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
package org.apache.nifi.processors.hive;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.LongHolder;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.hive.HiveJdbcCommon;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"hive", "sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("Execute provided HiveQL SELECT query against a Hive database connection. Query result will be converted to Avro or CSV format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'selecthiveql.row.count' indicates how many rows were selected.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the MIME type for the outgoing flowfile to application/avro-binary for Avro or text/csv for CSV."),
        @WritesAttribute(attribute = "filename", description = "Adds .avro or .csv to the filename attribute depending on which output format is selected."),
        @WritesAttribute(attribute = "selecthiveql.row.count", description = "Indicates how many rows were selected/returned by the query.")
})
public class SelectHiveQL extends AbstractHiveQLProcessor {

    public static final String RESULT_ROW_COUNT = "selecthiveql.row.count";

    protected static final String AVRO = "Avro";
    protected static final String CSV = "CSV";

    public static final String AVRO_MIME_TYPE = "application/avro-binary";
    public static final String CSV_MIME_TYPE = "text/csv";


    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from HiveQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("HiveQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();


    public static final PropertyDescriptor HIVEQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("hive-query")
            .displayName("HiveQL Select Query")
            .description("HiveQL SELECT query to execute")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor HIVEQL_OUTPUT_FORMAT = new PropertyDescriptor.Builder()
            .name("hive-output-format")
            .displayName("Output Format")
            .description("How to represent the records coming from Hive (Avro, CSV, e.g.)")
            .required(true)
            .allowableValues(AVRO, CSV)
            .defaultValue(AVRO)
            .expressionLanguageSupported(false)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        _propertyDescriptors.add(HIVEQL_SELECT_QUERY);
        _propertyDescriptors.add(HIVEQL_OUTPUT_FORMAT);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ProcessorLog logger = getLogger();
        final HiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(HiveDBCPService.class);
        final String selectQuery = context.getProperty(HIVEQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        final String outputFormat = context.getProperty(HIVEQL_OUTPUT_FORMAT).getValue();
        final StopWatch stopWatch = new StopWatch(true);

        try (final Connection con = dbcpService.getConnection();
             final Statement st = con.createStatement()) {
            final LongHolder nrOfRows = new LongHolder(0L);
            if (fileToProcess == null) {
                fileToProcess = session.create();
            }
            fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try {
                        logger.debug("Executing query {}", new Object[]{selectQuery});
                        final ResultSet resultSet = st.executeQuery(selectQuery);
                        if (AVRO.equals(outputFormat)) {
                            nrOfRows.set(HiveJdbcCommon.convertToAvroStream(resultSet, out));
                        } else if (CSV.equals(outputFormat)) {
                            nrOfRows.set(HiveJdbcCommon.convertToCsvStream(resultSet, out));
                        } else {
                            nrOfRows.set(0L);
                            throw new ProcessException("Unsupported output format: " + outputFormat);
                        }
                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }
                }
            });

            // set attribute how many rows were selected
            fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, nrOfRows.get().toString());

            // Set MIME type on output document and add extension
            if (AVRO.equals(outputFormat)) {
                fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), AVRO_MIME_TYPE);
                fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.FILENAME.key(), fileToProcess.getAttribute(CoreAttributes.FILENAME.key()) + ".avro");
            } else if (CSV.equals(outputFormat)) {
                fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);
                fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.FILENAME.key(), fileToProcess.getAttribute(CoreAttributes.FILENAME.key()) + ".csv");
            }

            logger.info("{} contains {} Avro records; transferring to 'success'",
                    new Object[]{fileToProcess, nrOfRows.get()});

            if (context.hasIncomingConnection()) {
                // If the flow file came from an incoming connection, issue a Modify Content provenance event

                session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + nrOfRows.get() + " rows",
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            } else {
                // If we created a flow file from rows received from Hive, issue a Receive provenance event
                session.getProvenanceReporter().receive(fileToProcess, dbcpService.getConnectionURL(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            }
            session.transfer(fileToProcess, REL_SUCCESS);
        } catch (final ProcessException | SQLException e) {
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute HiveQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute HiveQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, fileToProcess, e});
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute HiveQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.transfer(fileToProcess, REL_FAILURE);
            }
        }
    }
}
