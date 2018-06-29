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
package org.apache.nifi.processors.standard;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.JdbcCommon;
import org.apache.nifi.util.StopWatch;

import static org.apache.nifi.processors.standard.util.JdbcCommon.DEFAULT_PRECISION;
import static org.apache.nifi.processors.standard.util.JdbcCommon.DEFAULT_SCALE;
import static org.apache.nifi.processors.standard.util.JdbcCommon.NORMALIZE_NAMES_FOR_AVRO;
import static org.apache.nifi.processors.standard.util.JdbcCommon.USE_AVRO_LOGICAL_TYPES;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("Executes provided SQL select query. Query result will be converted to Avro format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query, and the query may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
        + "with the naming convention sql.args.N.type and sql.args.N.value, where N is a positive integer. The sql.args.N.type is expected to be "
        + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format. "
        + "FlowFile attribute 'executesql.row.count' indicates how many rows were selected.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "sql.args.N.type", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "sql.args.N.value", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The value of the Parameters are specified as "
                + "sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. The type of the sql.args.1.value Parameter is specified by the sql.args.1.type attribute."),
        @ReadsAttribute(attribute = "sql.args.N.format", description = "This attribute is always optional, but default options may not always work for your data. "
                + "Incoming FlowFiles are expected to be parametrized SQL statements. In some cases "
                + "a format option needs to be specified, currently this is only applicable for binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - "
                + "ascii: each string character in your attribute value represents a single byte. This is the format provided by Avro Processors. "
                + "base64: the string is a Base64 encoded string that can be decoded to bytes. "
                + "hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. "
                + "Dates/Times/Timestamps - "
                + "Date, Time and Timestamp formats all support both custom formats or named format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') "
                + "as specified according to java.time.format.DateTimeFormatter. "
                + "If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), or a string value in "
                + "'yyyy-MM-dd' format for Date, 'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds and will truncate milliseconds), "
                + "'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used.")
})
@WritesAttributes({
    @WritesAttribute(attribute="executesql.row.count", description = "Contains the number of rows returned in the select query"),
    @WritesAttribute(attribute="executesql.query.duration", description = "Combined duration of the query execution time and fetch time in milliseconds"),
    @WritesAttribute(attribute="executesql.query.executiontime", description = "Duration of the query execution time in milliseconds"),
    @WritesAttribute(attribute="executesql.query.fetchtime", description = "Duration of the result set fetch time in milliseconds"),
    @WritesAttribute(attribute="executesql.resultset.index", description = "Assuming multiple result sets are returned, "
       + "the zero based index of this result set."),
    @WritesAttribute(attribute="fragment.identifier", description="If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
            + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
    @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
            + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
            + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
            + "attribute will not be populated."),
    @WritesAttribute(attribute="fragment.index", description="If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
            + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
            + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced")
})
public class ExecuteSQL extends AbstractProcessor {

    public static final String RESULT_ROW_COUNT = "executesql.row.count";
    public static final String RESULT_QUERY_DURATION = "executesql.query.duration";
    public static final String RESULT_QUERY_EXECUTION_TIME = "executesql.query.executiontime";
    public static final String RESULT_QUERY_FETCH_TIME = "executesql.query.fetchtime";
    public static final String RESULTSET_INDEX = "executesql.resultset.index";

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();
    private final Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL select query")
            .description("The SQL select query to execute. The query can be empty, a constant value, or built from attributes "
                    + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                    + "incoming flowfiles. If this property is empty, the content of the incoming flow file is expected "
                    + "to contain a valid SQL select query, to be issued by the processor to the database. Note that Expression "
                    + "Language is not evaluated for flow file contents.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("esql-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The fragment.count attribute will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private final List<PropertyDescriptor> propDescriptors;

    public ExecuteSQL() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(SQL_SELECT_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(NORMALIZE_NAMES_FOR_AVRO);
        pds.add(USE_AVRO_LOGICAL_TYPES);
        pds.add(DEFAULT_PRECISION);
        pds.add(DEFAULT_SCALE);
        pds.add(MAX_ROWS_PER_FLOW_FILE);
        pds.add(OUTPUT_BATCH_SIZE);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
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

        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final Boolean useAvroLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer defaultPrecision = context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, Charset.defaultCharset())));
            selectQuery = queryContents.toString();
        }

        int resultCount=0;
        try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
            final PreparedStatement st = con.prepareStatement(selectQuery)) {
            st.setQueryTimeout(queryTimeout); // timeout in seconds

            if (fileToProcess != null) {
                JdbcCommon.setParameters(st, fileToProcess.getAttributes());
            }
            logger.debug("Executing query {}", new Object[]{selectQuery});

            int fragmentIndex=0;
            final String fragmentId = UUID.randomUUID().toString();

            final StopWatch executionTime = new StopWatch(true);

            boolean hasResults = st.execute();

            long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);

            boolean hasUpdateCount = st.getUpdateCount() != -1;

            while(hasResults || hasUpdateCount) {
                //getMoreResults() and execute() return false to indicate that the result of the statement is just a number and not a ResultSet
                if (hasResults) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    try {
                        final ResultSet resultSet = st.getResultSet();
                        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                                .convertNames(convertNamesForAvro)
                                .useLogicalTypes(useAvroLogicalTypes)
                                .defaultPrecision(defaultPrecision)
                                .defaultScale(defaultScale)
                                .maxRows(maxRowsPerFlowFile)
                                .build();

                        do {
                            final StopWatch fetchTime = new StopWatch(true);

                            FlowFile resultSetFF;
                            if (fileToProcess == null) {
                                resultSetFF = session.create();
                            } else {
                                resultSetFF = session.create(fileToProcess);
                                resultSetFF = session.putAllAttributes(resultSetFF, fileToProcess.getAttributes());
                            }

                            resultSetFF = session.write(resultSetFF, out -> {
                                try {
                                    nrOfRows.set(JdbcCommon.convertToAvroStream(resultSet, out, options, null));
                                } catch (SQLException e) {
                                    throw new ProcessException(e);
                                }
                            });

                            long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);

                            // set attribute how many rows were selected
                            resultSetFF = session.putAttribute(resultSetFF, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                            resultSetFF = session.putAttribute(resultSetFF, RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
                            resultSetFF = session.putAttribute(resultSetFF, RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
                            resultSetFF = session.putAttribute(resultSetFF, RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));
                            resultSetFF = session.putAttribute(resultSetFF, CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);
                            resultSetFF = session.putAttribute(resultSetFF, RESULTSET_INDEX, String.valueOf(resultCount));

                            // if fragmented ResultSet, determine if we should keep this fragment; set fragment attributes
                            if(maxRowsPerFlowFile > 0) {
                                // if row count is zero and this is not the first fragment, drop it instead of committing it.
                                if(nrOfRows.get() == 0 && fragmentIndex > 0) {
                                    session.remove(resultSetFF);
                                    break;
                                }

                                resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_ID, fragmentId);
                                resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                            }

                            logger.info("{} contains {} Avro records; transferring to 'success'",
                                    new Object[]{resultSetFF, nrOfRows.get()});
                            session.getProvenanceReporter().modifyContent(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                            resultSetFlowFiles.add(resultSetFF);

                            // If we've reached the batch size, send out the flow files
                            if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                                session.transfer(resultSetFlowFiles, REL_SUCCESS);
                                session.commit();
                                resultSetFlowFiles.clear();
                            }

                            fragmentIndex++;
                        } while (maxRowsPerFlowFile > 0 && nrOfRows.get() == maxRowsPerFlowFile);

                        // If we are splitting results but not outputting batches, set count on all FlowFiles
                        if (outputBatchSize == 0 && maxRowsPerFlowFile > 0) {
                            for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                                resultSetFlowFiles.set(i,
                                        session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                            }
                        }
                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }

                    resultCount++;
                }

                // are there anymore result sets?
                try{
                    hasResults = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                    hasUpdateCount = st.getUpdateCount() != -1;
                } catch(SQLException ex){
                    hasResults = false;
                    hasUpdateCount = false;
                }
            }

            // Transfer any remaining files to SUCCESS
            session.transfer(resultSetFlowFiles, REL_SUCCESS);
            resultSetFlowFiles.clear();

            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if(fileToProcess != null){
                if(resultCount > 0){
                    session.remove(fileToProcess);
                } else {
                    fileToProcess = session.write(fileToProcess, JdbcCommon::createEmptyAvroStream);

                    fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, "0");
                    fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);
                    session.transfer(fileToProcess, REL_SUCCESS);
                }
            } else if(resultCount == 0){
                //If we had no inbound FlowFile, no exceptions, and the SQL generated no result sets (Insert/Update/Delete statements only)
                // Then generate an empty Output FlowFile
                FlowFile resultSetFF = session.create();

                resultSetFF = session.write(resultSetFF, out -> JdbcCommon.createEmptyAvroStream(out));

                resultSetFF = session.putAttribute(resultSetFF, RESULT_ROW_COUNT, "0");
                resultSetFF = session.putAttribute(resultSetFF, CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);
                session.transfer(resultSetFF, REL_SUCCESS);
            }
        } catch (final ProcessException | SQLException e) {
            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute SQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, fileToProcess, e});
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute SQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.transfer(fileToProcess, REL_FAILURE);
            }
        }
    }
}
