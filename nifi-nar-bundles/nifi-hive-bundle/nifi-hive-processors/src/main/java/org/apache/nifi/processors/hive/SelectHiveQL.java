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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.PartialFunctions;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.hive.CsvOutputOptions;
import org.apache.nifi.util.hive.HiveJdbcCommon;

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
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor HIVEQL_CSV_HEADER = new PropertyDescriptor.Builder()
            .name("csv-header")
            .displayName("CSV Header")
            .description("Include Header in Output")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor HIVEQL_CSV_ALT_HEADER = new PropertyDescriptor.Builder()
            .name("csv-alt-header")
            .displayName("Alternate CSV Header")
            .description("Comma separated list of header fields")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor HIVEQL_CSV_DELIMITER = new PropertyDescriptor.Builder()
            .name("csv-delimiter")
            .displayName("CSV Delimiter")
            .description("CSV Delimiter used to separate fields")
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor HIVEQL_CSV_QUOTE = new PropertyDescriptor.Builder()
            .name("csv-quote")
            .displayName("CSV Quote")
            .description("Whether to force quoting of CSV fields. Note that this might conflict with the setting for CSV Escape.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor HIVEQL_CSV_ESCAPE = new PropertyDescriptor.Builder()
            .name("csv-escape")
            .displayName("CSV Escape")
            .description("Whether to escape CSV strings in output. Note that this might conflict with the setting for CSV Quote.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
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
        _propertyDescriptors.add(HIVEQL_CSV_HEADER);
        _propertyDescriptors.add(HIVEQL_CSV_ALT_HEADER);
        _propertyDescriptors.add(HIVEQL_CSV_DELIMITER);
        _propertyDescriptors.add(HIVEQL_CSV_QUOTE);
        _propertyDescriptors.add(HIVEQL_CSV_ESCAPE);
        _propertyDescriptors.add(CHARSET);
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

    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(HIVEQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        PartialFunctions.onTrigger(context, sessionFactory, getLogger(), session -> onTrigger(context, session));
    }

    private void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile fileToProcess = (context.hasIncomingConnection()? session.get():null);
        FlowFile flowfile = null;

        // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
        // However, if we have no FlowFile and we have connections coming from other Processors, then
        // we know that we should run only if we have a FlowFile.
        if (context.hasIncomingConnection()) {
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();
        final HiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(HiveDBCPService.class);
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());

        final boolean flowbased = !(context.getProperty(HIVEQL_SELECT_QUERY).isSet());

        // Source the SQL
        final String selectQuery;

        if (context.getProperty(HIVEQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(HIVEQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    queryContents.append(IOUtils.toString(in));
                }
            });
            selectQuery = queryContents.toString();
        }


        final String outputFormat = context.getProperty(HIVEQL_OUTPUT_FORMAT).getValue();
        final StopWatch stopWatch = new StopWatch(true);
        final boolean header = context.getProperty(HIVEQL_CSV_HEADER).asBoolean();
        final String altHeader = context.getProperty(HIVEQL_CSV_ALT_HEADER).evaluateAttributeExpressions(fileToProcess).getValue();
        final String delimiter = context.getProperty(HIVEQL_CSV_DELIMITER).evaluateAttributeExpressions(fileToProcess).getValue();
        final boolean quote = context.getProperty(HIVEQL_CSV_QUOTE).asBoolean();
        final boolean escape = context.getProperty(HIVEQL_CSV_HEADER).asBoolean();

        try (final Connection con = dbcpService.getConnection();
             final Statement st = ( flowbased ? con.prepareStatement(selectQuery): con.createStatement())
        ) {

            final AtomicLong nrOfRows = new AtomicLong(0L);
            if (fileToProcess == null) {
                flowfile = session.create();
            } else {
                flowfile = fileToProcess;
            }

            flowfile = session.write(flowfile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try {
                        logger.debug("Executing query {}", new Object[]{selectQuery});
                        if (flowbased) {
                            // Hive JDBC Doesn't Support this yet:
                            // ParameterMetaData pmd = ((PreparedStatement)st).getParameterMetaData();
                            // int paramCount = pmd.getParameterCount();

                            // Alternate way to determine number of params in SQL.
                            int paramCount = StringUtils.countMatches(selectQuery, "?");

                            if (paramCount > 0) {
                                setParameters(1, (PreparedStatement) st, paramCount, fileToProcess.getAttributes());
                            }
                        }

                        final ResultSet resultSet = (flowbased ? ((PreparedStatement)st).executeQuery(): st.executeQuery(selectQuery));

                        if (AVRO.equals(outputFormat)) {
                            nrOfRows.set(HiveJdbcCommon.convertToAvroStream(resultSet, out));
                        } else if (CSV.equals(outputFormat)) {
                            CsvOutputOptions options = new CsvOutputOptions(header, altHeader, delimiter, quote, escape);
                            nrOfRows.set(HiveJdbcCommon.convertToCsvStream(resultSet, out,options));
                        } else {
                            nrOfRows.set(0L);
                            throw new ProcessException("Unsupported output format: " + outputFormat);
                        }
                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }
                }
            });

            // Set attribute for how many rows were selected
            flowfile = session.putAttribute(flowfile, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));

            // Set MIME type on output document and add extension to filename
            if (AVRO.equals(outputFormat)) {
                flowfile = session.putAttribute(flowfile, CoreAttributes.MIME_TYPE.key(), AVRO_MIME_TYPE);
                flowfile = session.putAttribute(flowfile, CoreAttributes.FILENAME.key(), flowfile.getAttribute(CoreAttributes.FILENAME.key()) + ".avro");
            } else if (CSV.equals(outputFormat)) {
                flowfile = session.putAttribute(flowfile, CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);
                flowfile = session.putAttribute(flowfile, CoreAttributes.FILENAME.key(), flowfile.getAttribute(CoreAttributes.FILENAME.key()) + ".csv");
            }

            logger.info("{} contains {} Avro records; transferring to 'success'",
                    new Object[]{flowfile, nrOfRows.get()});

            if (context.hasIncomingConnection()) {
                // If the flow file came from an incoming connection, issue a Modify Content provenance event

                session.getProvenanceReporter().modifyContent(flowfile, "Retrieved " + nrOfRows.get() + " rows",
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            } else {
                // If we created a flow file from rows received from Hive, issue a Receive provenance event
                session.getProvenanceReporter().receive(flowfile, dbcpService.getConnectionURL(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            }
            session.transfer(flowfile, REL_SUCCESS);
        } catch (final ProcessException | SQLException e) {
            logger.error("Issue processing SQL {} due to {}.", new Object[]{selectQuery, e});
            if (flowfile == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute HiveQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute HiveQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, flowfile, e});
                    flowfile = session.penalize(flowfile);
                } else {
                    logger.error("Unable to execute HiveQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.transfer(flowfile, REL_FAILURE);
            }
        } finally {

        }
    }
}
