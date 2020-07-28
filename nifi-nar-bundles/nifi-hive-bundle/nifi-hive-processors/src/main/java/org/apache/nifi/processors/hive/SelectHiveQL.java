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

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.PartialFunctions;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.hive.CsvOutputOptions;
import org.apache.nifi.util.hive.HiveJdbcCommon;

import static org.apache.nifi.util.hive.HiveJdbcCommon.AVRO;
import static org.apache.nifi.util.hive.HiveJdbcCommon.CSV;
import static org.apache.nifi.util.hive.HiveJdbcCommon.CSV_MIME_TYPE;
import static org.apache.nifi.util.hive.HiveJdbcCommon.MIME_TYPE_AVRO_BINARY;
import static org.apache.nifi.util.hive.HiveJdbcCommon.NORMALIZE_NAMES_FOR_AVRO;

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
        @WritesAttribute(attribute = "selecthiveql.row.count", description = "Indicates how many rows were selected/returned by the query."),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "query.input.tables", description = "Contains input table names in comma delimited 'databaseName.tableName' format.")
})
public class SelectHiveQL extends AbstractHiveQLProcessor {

    public static final String RESULT_ROW_COUNT = "selecthiveql.row.count";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from HiveQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("HiveQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship.")
            .build();


    public static final PropertyDescriptor HIVEQL_PRE_QUERY = new PropertyDescriptor.Builder()
            .name("hive-pre-query")
            .displayName("HiveQL Pre-Query")
            .description("A semicolon-delimited list of queries executed before the main SQL query is executed. "
                    + "Example: 'set tez.queue.name=queue1; set hive.exec.orc.split.strategy=ETL; set hive.exec.reducers.bytes.per.reducer=1073741824'. "
                    + "Note, the results/outputs of these queries will be suppressed if successfully executed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor HIVEQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("hive-query")
            .displayName("HiveQL Select Query")
            .description("HiveQL SELECT query to execute. If this is not set, the query is assumed to be in the content of an incoming FlowFile.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor HIVEQL_POST_QUERY = new PropertyDescriptor.Builder()
            .name("hive-post-query")
            .displayName("HiveQL Post-Query")
            .description("A semicolon-delimited list of queries executed after the main SQL query is executed. "
                    + "Note, the results/outputs of these queries will be suppressed if successfully executed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("hive-fetch-size")
            .displayName("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("hive-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. " +
                    "This will allow you to break up very large result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_FRAGMENTS = new PropertyDescriptor.Builder()
            .name("hive-max-frags")
            .displayName("Maximum Number of Fragments")
            .description("The maximum number of fragments. If the value specified is zero, then all fragments are returned. " +
                    "This prevents OutOfMemoryError when this processor ingests huge table.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor HIVEQL_CSV_DELIMITER = new PropertyDescriptor.Builder()
            .name("csv-delimiter")
            .displayName("CSV Delimiter")
            .description("CSV Delimiter used to separate fields")
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
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
        _propertyDescriptors.add(HIVEQL_PRE_QUERY);
        _propertyDescriptors.add(HIVEQL_SELECT_QUERY);
        _propertyDescriptors.add(HIVEQL_POST_QUERY);
        _propertyDescriptors.add(FETCH_SIZE);
        _propertyDescriptors.add(MAX_ROWS_PER_FLOW_FILE);
        _propertyDescriptors.add(MAX_FRAGMENTS);
        _propertyDescriptors.add(HIVEQL_OUTPUT_FORMAT);
        _propertyDescriptors.add(NORMALIZE_NAMES_FOR_AVRO);
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
        FlowFile fileToProcess = (context.hasIncomingConnection() ? session.get() : null);
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

        List<String> preQueries = getQueries(context.getProperty(HIVEQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
        List<String> postQueries = getQueries(context.getProperty(HIVEQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());

        final boolean flowbased = !(context.getProperty(HIVEQL_SELECT_QUERY).isSet());

        // Source the SQL
        String hqlStatement;

        if (context.getProperty(HIVEQL_SELECT_QUERY).isSet()) {
            hqlStatement = context.getProperty(HIVEQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, charset)));
            hqlStatement = queryContents.toString();
        }


        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions(fileToProcess).asInteger()
                : 0;
        final String outputFormat = context.getProperty(HIVEQL_OUTPUT_FORMAT).getValue();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final StopWatch stopWatch = new StopWatch(true);
        final boolean header = context.getProperty(HIVEQL_CSV_HEADER).asBoolean();
        final String altHeader = context.getProperty(HIVEQL_CSV_ALT_HEADER).evaluateAttributeExpressions(fileToProcess).getValue();
        final String delimiter = context.getProperty(HIVEQL_CSV_DELIMITER).evaluateAttributeExpressions(fileToProcess).getValue();
        final boolean quote = context.getProperty(HIVEQL_CSV_QUOTE).asBoolean();
        final boolean escape = context.getProperty(HIVEQL_CSV_HEADER).asBoolean();
        final String fragmentIdentifier = UUID.randomUUID().toString();

        try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
             final Statement st = (flowbased ? con.prepareStatement(hqlStatement) : con.createStatement())
        ) {
            Pair<String,SQLException> failure = executeConfigStatements(con, preQueries);
            if (failure != null) {
                // In case of failure, assigning config query to "hqlStatement"  to follow current error handling
                hqlStatement = failure.getLeft();
                flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                fileToProcess = null;
                throw failure.getRight();
            }
            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }

            final List<FlowFile> resultSetFlowFiles = new ArrayList<>();
            try {
                logger.debug("Executing query {}", new Object[]{hqlStatement});
                if (flowbased) {
                    // Hive JDBC Doesn't Support this yet:
                    // ParameterMetaData pmd = ((PreparedStatement)st).getParameterMetaData();
                    // int paramCount = pmd.getParameterCount();

                    // Alternate way to determine number of params in SQL.
                    int paramCount = StringUtils.countMatches(hqlStatement, "?");

                    if (paramCount > 0) {
                        setParameters(1, (PreparedStatement) st, paramCount, fileToProcess.getAttributes());
                    }
                }

                final ResultSet resultSet;

                try {
                    resultSet = (flowbased ? ((PreparedStatement) st).executeQuery() : st.executeQuery(hqlStatement));
                } catch (SQLException se) {
                    // If an error occurs during the query, a flowfile is expected to be routed to failure, so ensure one here
                    flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                    fileToProcess = null;
                    throw se;
                }

                int fragmentIndex = 0;
                String baseFilename = (fileToProcess != null) ? fileToProcess.getAttribute(CoreAttributes.FILENAME.key()) : null;
                while (true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);
                    flowfile = (fileToProcess == null) ? session.create() : session.create(fileToProcess);
                    if (baseFilename == null) {
                        baseFilename = flowfile.getAttribute(CoreAttributes.FILENAME.key());
                    }
                    try {
                        flowfile = session.write(flowfile, out -> {
                            try {
                                if (AVRO.equals(outputFormat)) {
                                    nrOfRows.set(HiveJdbcCommon.convertToAvroStream(resultSet, out, maxRowsPerFlowFile, convertNamesForAvro));
                                } else if (CSV.equals(outputFormat)) {
                                    CsvOutputOptions options = new CsvOutputOptions(header, altHeader, delimiter, quote, escape, maxRowsPerFlowFile);
                                    nrOfRows.set(HiveJdbcCommon.convertToCsvStream(resultSet, out, options));
                                } else {
                                    nrOfRows.set(0L);
                                    throw new ProcessException("Unsupported output format: " + outputFormat);
                                }
                            } catch (final SQLException | RuntimeException e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(flowfile);
                        throw e;
                    }

                    if (nrOfRows.get() > 0 || resultSetFlowFiles.isEmpty()) {
                        final Map<String, String> attributes = new HashMap<>();
                        // Set attribute for how many rows were selected
                        attributes.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));

                        try {
                            // Set input/output table names by parsing the query
                            attributes.putAll(toQueryTableAttributes(findTableNames(hqlStatement)));
                        } catch (Exception e) {
                            // If failed to parse the query, just log a warning message, but continue.
                            getLogger().warn("Failed to parse query: {} due to {}", new Object[]{hqlStatement, e}, e);
                        }

                        // Set MIME type on output document and add extension to filename
                        if (AVRO.equals(outputFormat)) {
                            attributes.put(CoreAttributes.MIME_TYPE.key(), MIME_TYPE_AVRO_BINARY);
                            attributes.put(CoreAttributes.FILENAME.key(), baseFilename + "." + fragmentIndex + ".avro");
                        } else if (CSV.equals(outputFormat)) {
                            attributes.put(CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);
                            attributes.put(CoreAttributes.FILENAME.key(), baseFilename + "." + fragmentIndex + ".csv");
                        }

                        if (maxRowsPerFlowFile > 0) {
                            attributes.put("fragment.identifier", fragmentIdentifier);
                            attributes.put("fragment.index", String.valueOf(fragmentIndex));
                        }

                        flowfile = session.putAllAttributes(flowfile, attributes);

                        logger.info("{} contains {} " + outputFormat + " records; transferring to 'success'",
                                new Object[]{flowfile, nrOfRows.get()});

                        if (context.hasIncomingConnection()) {
                            // If the flow file came from an incoming connection, issue a Fetch provenance event
                            session.getProvenanceReporter().fetch(flowfile, dbcpService.getConnectionURL(),
                                    "Retrieved " + nrOfRows.get() + " rows", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        } else {
                            // If we created a flow file from rows received from Hive, issue a Receive provenance event
                            session.getProvenanceReporter().receive(flowfile, dbcpService.getConnectionURL(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        }
                        resultSetFlowFiles.add(flowfile);
                    } else {
                        // If there were no rows returned (and the first flow file has been sent, we're done processing, so remove the flowfile and carry on
                        session.remove(flowfile);
                        if (resultSetFlowFiles != null && resultSetFlowFiles.size()>0) {
                            flowfile = resultSetFlowFiles.get(resultSetFlowFiles.size()-1);
                        }
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }
                }

                for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                    // Set count on all FlowFiles
                    if (maxRowsPerFlowFile > 0) {
                        resultSetFlowFiles.set(i,
                                session.putAttribute(resultSetFlowFiles.get(i), "fragment.count", Integer.toString(fragmentIndex)));
                    }
                }

            } catch (final SQLException e) {
                throw e;
            }

            failure = executeConfigStatements(con, postQueries);
            if (failure != null) {
                hqlStatement = failure.getLeft();
                if (resultSetFlowFiles != null) {
                    resultSetFlowFiles.forEach(ff -> session.remove(ff));
                }
                flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                fileToProcess = null;
                throw failure.getRight();
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);
            if (fileToProcess != null) {
                session.remove(fileToProcess);
            }
        } catch (final ProcessException | SQLException e) {
            logger.error("Issue processing SQL {} due to {}.", new Object[]{hqlStatement, e});
            if (flowfile == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute HiveQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{hqlStatement, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute HiveQL select query {} for {} due to {}; routing to failure",
                            new Object[]{hqlStatement, flowfile, e});
                    flowfile = session.penalize(flowfile);
                } else {
                    logger.error("Unable to execute HiveQL select query {} due to {}; routing to failure",
                            new Object[]{hqlStatement, e});
                    context.yield();
                }
                session.transfer(flowfile, REL_FAILURE);
            }
        }
    }

    /*
     * Executes given queries using pre-defined connection.
     * Returns null on success, or a query string if failed.
     */
    protected Pair<String,SQLException> executeConfigStatements(final Connection con, final List<String> configQueries){
        if (configQueries == null || configQueries.isEmpty()) {
            return null;
        }

        for (String confSQL : configQueries) {
            try(final Statement st = con.createStatement()){
                st.execute(confSQL);
            } catch (SQLException e) {
                return Pair.of(confSQL, e);
            }
        }
        return null;
    }

    protected List<String> getQueries(final String value) {
        if (value == null || value.length() == 0 || value.trim().length() == 0) {
            return null;
        }
        final List<String> queries = new LinkedList<>();
        for (String query : value.split(";")) {
            if (query.trim().length() > 0) {
                queries.add(query.trim());
            }
        }
        return queries;
    }
}