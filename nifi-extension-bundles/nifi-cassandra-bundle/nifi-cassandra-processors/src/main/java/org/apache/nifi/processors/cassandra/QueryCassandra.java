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
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Tags({"cassandra", "cql", "select"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Execute provided Cassandra Query Language (CQL) select query on a Cassandra 1.x, 2.x, or 3.0.x cluster. Query result "
        + "may be converted to Avro or JSON format. Streaming is used so arbitrarily large result sets are supported. This processor can be "
        + "scheduled to run on a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'executecql.row.count' indicates how many rows were selected.")
@WritesAttributes({
        @WritesAttribute(attribute = "executecql.row.count", description = "The number of rows returned by the CQL query"),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced")
})
public class QueryCassandra extends AbstractCassandraProcessor {

    public static final String AVRO_FORMAT = "Avro";
    public static final String JSON_FORMAT = "JSON";

    public static final String RESULT_ROW_COUNT = "executecql.row.count";

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

    private static final byte[] OPEN_OBJ  = "{".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CLOSE_OBJ = "}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMA     = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COLON     = ":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] QUOTE     = "\"".getBytes(StandardCharsets.UTF_8);

    private static final byte[] RESULTS_PREFIX = "{\"results\":[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY_RESULTS = "{\"results\":[]}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RESULTS_SUFFIX = "]}".getBytes(StandardCharsets.UTF_8);


    public static final PropertyDescriptor CQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("CQL Select Query")
            .description("CQL select query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running CQL select query. Must be of format "
                    + "<duration> <TimeUnit> where <duration> is a non-negative integer and TimeUnit is a supported "
                    + "Time Unit, such as: millis, secs, mins, hrs, days. A value of zero means there is no limit. ")
            .defaultValue("0 seconds")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. Zero is the default "
                    + "and means there is no limit.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The maxvalue.* and fragment.count attributes will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor.Builder()
            .name("Output Format")
            .description("The format to which the result rows will be converted. If JSON is selected, the output will "
                    + "contain an object with field 'results' containing an array of result rows. Each row in the array is a "
                    + "map of the named column to its value. For example: { \"results\": [{\"userid\":1, \"name\":\"Joe Smith\"}]}")
            .required(true)
            .allowableValues(AVRO_FORMAT, JSON_FORMAT)
            .defaultValue(AVRO_FORMAT)
            .build();

    public static final PropertyDescriptor TIMESTAMP_FORMAT_PATTERN = new PropertyDescriptor.Builder()
            .name("Timestamp Format Pattern for JSON output")
            .description("Pattern to use when converting timestamp fields to JSON. Note: the formatted timestamp will be in UTC timezone.")
            .required(true)
            .defaultValue("yyyy-MM-dd HH:mm:ssZ")
            .addValidator((subject, input, context) -> {
                final ValidationResult.Builder vrb = new ValidationResult.Builder().subject(subject).input(input);
                try {
                    new SimpleDateFormat(input).format(new Date());
                    vrb.valid(true).explanation("Valid date format pattern");
                } catch (Exception ex) {
                    vrb.valid(false).explanation("the pattern is invalid: " + ex.getMessage());
                }
                return vrb.build();
            })
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            Stream.concat(
                    COMMON_PROPERTY_DESCRIPTORS.stream(),
                    Stream.of(
                            CQL_SELECT_QUERY,
                            QUERY_TIMEOUT,
                            FETCH_SIZE,
                            MAX_ROWS_PER_FLOW_FILE,
                            OUTPUT_BATCH_SIZE,
                            OUTPUT_FORMAT,
                            TIMESTAMP_FORMAT_PATTERN
                    )
            ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_RETRY
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        boolean hasIncomingConnection = context.hasIncomingConnection();

        if (hasIncomingConnection) {
            fileToProcess = session.get();
            // If we have no FlowFile and connections are from other processors, stop.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();
        final String selectQuery = context.getProperty(CQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        final long queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.MILLISECONDS);
        final String outputFormat = context.getProperty(OUTPUT_FORMAT).getValue();
        final long maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final long outputBatchSize = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(fileToProcess).getValue());
        final StopWatch stopWatch = new StopWatch(true);
        final int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();

        final List<FlowFile> resultSetFlowFiles = new LinkedList<>();

        try {
            final CqlSession cqlSession = cassandraSession.get();

            SimpleStatementBuilder stmtBuilder = SimpleStatement.builder(selectQuery);

            if (maxRowsPerFlowFile > 0) {
                stmtBuilder.setPageSize((int) maxRowsPerFlowFile);
            } else if (fetchSize > 0) {
                stmtBuilder.setPageSize(fetchSize);
            }

            if (queryTimeout > 0) {
                stmtBuilder.setTimeout(Duration.ofMillis(queryTimeout));
            }

            AsyncResultSet asyncResultSet = cqlSession.executeAsync(stmtBuilder.build()).toCompletableFuture().get();
            final AtomicLong rowsInCurrentFlowFile = new AtomicLong(0L);

            if (fileToProcess == null) {
                fileToProcess = session.create();
            }

            int fragmentIndex = 0;
            final String fragmentId = UUID.randomUUID().toString();

            while (asyncResultSet != null) {
                final AsyncResultSet currentPage = asyncResultSet;
                final int currentFragmentIndex = fragmentIndex;
                final Iterable<Row> pageRows = currentPage.currentPage();
                final boolean isLastPage = !currentPage.hasMorePages();

                if (maxRowsPerFlowFile > 0) {
                    fileToProcess = session.write(fileToProcess, out -> {
                        try {
                            rowsInCurrentFlowFile.set(0L);
                            if (AVRO_FORMAT.equals(outputFormat)) {
                                rowsInCurrentFlowFile.set(convertToAvroStream(pageRows, maxRowsPerFlowFile, out));
                            } else {
                                rowsInCurrentFlowFile.set(convertToJsonStream(pageRows, maxRowsPerFlowFile, out, charset));
                            }
                        } catch (Exception e) {
                            throw new ProcessException(e);
                        }
                    });
                } else {
                    fileToProcess = session.append(fileToProcess, out -> {
                        try {
                            long added = (AVRO_FORMAT.equals(outputFormat))
                                    ? convertToAvroStream(pageRows, maxRowsPerFlowFile, out)
                                    : convertToJsonStream(pageRows, maxRowsPerFlowFile, out, charset);
                            rowsInCurrentFlowFile.addAndGet(added); // Accumulate total count
                        } catch (Exception e) {
                            throw new ProcessException(e);
                        }
                    });
                }

                boolean shouldFinishFile = (maxRowsPerFlowFile > 0) || isLastPage;

                if (shouldFinishFile) {
                    long totalRows = rowsInCurrentFlowFile.get();
                    boolean keepFile = (totalRows > 0) || (hasIncomingConnection && resultSetFlowFiles.isEmpty());

                    if (keepFile) {
                        fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, String.valueOf(totalRows));
                        fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(),
                                JSON_FORMAT.equals(outputFormat) ? "application/json" : "application/avro-binary");

                        if (maxRowsPerFlowFile > 0) {
                            fileToProcess = session.putAttribute(fileToProcess, FRAGMENT_ID, fragmentId);
                            fileToProcess = session.putAttribute(fileToProcess, FRAGMENT_INDEX, String.valueOf(currentFragmentIndex));
                        }

                        session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + totalRows + " rows", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        resultSetFlowFiles.add(fileToProcess);

                        if (outputBatchSize > 0 && resultSetFlowFiles.size() == outputBatchSize) {
                            session.transfer(resultSetFlowFiles, REL_SUCCESS);
                            session.commitAsync();
                            resultSetFlowFiles.clear();
                        }

                        if (!isLastPage && maxRowsPerFlowFile > 0) {
                            fragmentIndex++;
                            fileToProcess = session.create();
                            rowsInCurrentFlowFile.set(0L);
                        } else {
                            fileToProcess = null;
                        }
                    } else {
                        session.remove(fileToProcess);
                        fileToProcess = null;
                    }
                }

                asyncResultSet = isLastPage ? null : currentPage.fetchNextPage().toCompletableFuture().get();
            }

            if (!resultSetFlowFiles.isEmpty()) {
                if (maxRowsPerFlowFile > 0) {
                    final String totalFragments = String.valueOf(fragmentIndex + 1);
                    for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                        resultSetFlowFiles.set(i, session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, totalFragments));
                    }
                }
                session.transfer(resultSetFlowFiles, REL_SUCCESS);
            }

        } catch (final QueryExecutionException qee) {
            logger.error("Cannot execute the query with the requested consistency level successfully", qee);
            if (fileToProcess == null) {
                fileToProcess = session.create();
            }
            fileToProcess = session.penalize(fileToProcess);
            session.transfer(fileToProcess, REL_RETRY);

        } catch (final QueryValidationException qve) {
            if (context.hasIncomingConnection()) {
                logger.error("The CQL query {} is invalid; routing {} to failure", selectQuery, fileToProcess, qve);
                if (fileToProcess == null) {
                    fileToProcess = session.create();
                }
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_FAILURE);
            } else {
                logger.error("The CQL query {} is invalid", selectQuery, qve);
                if (fileToProcess != null) {
                    session.remove(fileToProcess);
                }
                context.yield();
            }
        } catch (InterruptedException | ExecutionException ex) {
            Throwable cause = ex;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }

            if (cause instanceof AllNodesFailedException) {
                logger.error("All Cassandra nodes failed", cause);
                if (fileToProcess == null) {
                    fileToProcess = session.create();
                }
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_RETRY);
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("The CQL query {} yielded an unknown error; routing to failure", selectQuery, ex);
                    if (fileToProcess == null) {
                        fileToProcess = session.create();
                    }
                    fileToProcess = session.penalize(fileToProcess);
                    session.transfer(fileToProcess, REL_FAILURE);
                } else {
                    logger.error("The CQL query {} ran into an unknown error", selectQuery, ex);
                    if (fileToProcess != null) {
                        session.remove(fileToProcess);
                    }
                    context.yield();
                }
            }
        } catch (final ProcessException e) {
            if (context.hasIncomingConnection()) {
                logger.error("Unable to execute CQL query {}", selectQuery, e);
                if (fileToProcess == null) {
                    fileToProcess = session.create();
                }
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_FAILURE);
            } else {
                logger.error("Unable to execute CQL query {}", selectQuery, e);
                if (fileToProcess != null) {
                    session.remove(fileToProcess);
                }
                context.yield();
            }
        }
        session.commitAsync();
    }

    @OnUnscheduled
    @Override
    public void stop(ProcessContext context) {
        super.stop(context);
    }

    @OnShutdown
    public void shutdown(ProcessContext context) {
        super.stop(context);
    }

    /**
     * Converts a result set into an Avro record and writes it to the given stream.
     *
     * @param rowsPage        The result set to convert
     * @param outStream The stream to which the Avro record will be written
     * @return The number of rows from the result set written to the stream
     * @throws IOException          If the Avro record cannot be written
     * @throws InterruptedException If a result set fetch is interrupted
     * @throws TimeoutException     If a result set fetch has taken longer than the specified timeout
     * @throws ExecutionException   If any error occurs during the result set fetch
     */
    public static long convertToAvroStream(final Iterable<Row> rowsPage, long maxRowsPerFlowFile,
                                           final OutputStream outStream)
            throws IOException, InterruptedException, TimeoutException, ExecutionException {

        final Iterator<Row> iterator = rowsPage.iterator();
        if (!iterator.hasNext()) {
            return 0;
        }

        // Use the first row to create the schema
        Row firstRow = iterator.next();
        final Schema schema = createSchema(firstRow);
        final GenericRecord rec = new GenericData.Record(schema);
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            long nrOfRows = 0;

            for (int i = 0; i < schema.getFields().size(); i++) {
                rec.put(i, firstRow.isNull(i) ? null : getCassandraObject(firstRow, i));
            }
            dataFileWriter.append(rec);
            nrOfRows++;

            while (iterator.hasNext() && (maxRowsPerFlowFile == 0 || nrOfRows < maxRowsPerFlowFile)) {
                Row row = iterator.next();
                for (int i = 0; i < schema.getFields().size(); i++) {
                    rec.put(i, row.isNull(i) ? null : getCassandraObject(row, i));
                }
                dataFileWriter.append(rec);
                nrOfRows++;
            }

            return nrOfRows;
        }
    }

    private static String getFormattedDate(final Optional<ProcessContext> context, Date value) {
        final String dateFormatPattern = context
                .map(_context -> _context.getProperty(TIMESTAMP_FORMAT_PATTERN).getValue())
                .orElse(TIMESTAMP_FORMAT_PATTERN.getDefaultValue());
        SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(value);
    }

    public static long convertToJsonStream(final Iterable<Row> rowsPage, long maxRowsPerFlowFile,
                                           final OutputStream outStream,
                                           Charset charset)
            throws IOException, InterruptedException, TimeoutException, ExecutionException {
        return convertToJsonStream(Optional.empty(), rowsPage, maxRowsPerFlowFile, outStream, charset);
    }

    /**
     * Converts a result set into an Json object and writes it to the given stream using the specified character set.
     *
     * @param rowsPage An iterable page of Cassandra {@link Row} objects to be converted
     * @param outStream The stream to which the JSON object will be written
     * @return The number of rows from the result set written to the stream
     * @throws IOException If the JSON object cannot be written
     */
    public static long convertToJsonStream(final Optional<ProcessContext> context, final Iterable<Row> rowsPage,
                                           long maxRowsPerFlowFile,
                                           final OutputStream outStream,
                                           Charset charset) throws IOException {

        long nrOfRows = 0;
        Iterator<Row> iterator = rowsPage.iterator();
        if (!iterator.hasNext()) {
            outStream.write(EMPTY_RESULTS);
            return 0;
        }

        outStream.write(RESULTS_PREFIX);
        boolean firstRow = true;

        for (Row row : rowsPage) {
            if (maxRowsPerFlowFile > 0 && nrOfRows >= maxRowsPerFlowFile) {
                break;
            }

            if (!firstRow) {
                outStream.write(COMMA);
            } else {
                firstRow = false;
            }

            outStream.write(OPEN_OBJ);

            ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
            for (int i = 0; i < columnDefinitions.size(); i++) {
                if (i != 0) {
                    outStream.write(COMMA);
                }

                String colName = columnDefinitions.get(i).getName().asInternal();

                outStream.write(QUOTE);
                outStream.write(colName.getBytes(charset));
                outStream.write(QUOTE);
                outStream.write(COLON);

                Object value = row.isNull(i) ? null : getCassandraObject(row, i);

                String valueStr;
                if (value instanceof List || value instanceof Set) {
                    StringBuilder sb = new StringBuilder();
                    sb.append('[');
                    boolean first = true;
                    for (Object element : (Collection<?>) value) {
                        if (!first) {
                            sb.append(',');
                        }
                        sb.append(getJsonElement(context, element));
                        first = false;
                    }
                    sb.append(']');
                    valueStr = sb.toString();
                } else if (value instanceof Map) {
                    StringBuilder sb = new StringBuilder();
                    sb.append('{');
                    boolean first = true;
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                        if (!first) {
                            sb.append(',');
                        }
                        sb.append(getJsonElement(context, entry.getKey()))
                                .append(':')
                                .append(getJsonElement(context, entry.getValue()));
                        first = false;
                    }
                    sb.append('}');
                    valueStr = sb.toString();
                } else {
                    valueStr = getJsonElement(context, value);
                }
                outStream.write(valueStr.getBytes(charset));
            }
            outStream.write(CLOSE_OBJ);
            nrOfRows++;
        }

        outStream.write(RESULTS_SUFFIX);
        return nrOfRows;
    }

    protected static String getJsonElement(final Optional<ProcessContext> context, Object value) {
        final String QUOTE = "\"";
        String escapedValue;

        if (value == null) {
            escapedValue = "null";
        } else if (value instanceof Number) {
            escapedValue = value.toString();
        } else if (value instanceof Date) {
            escapedValue = getFormattedDate(context, (Date) value);
        } else if (value instanceof String) {
            escapedValue = StringEscapeUtils.escapeJson((String) value);
        } else {
            escapedValue = StringEscapeUtils.escapeJson(value.toString());
        }

        if (!(value instanceof Number || value == null)) {
            escapedValue = QUOTE + escapedValue + QUOTE;
        }

        return escapedValue;
    }

    /**
     * Creates an Avro schema from the given result set. The metadata (column definitions, data types, etc.) is used
     * to determine a schema for Avro.
     *
     * @param row The result set from which an Avro schema will be created
     * @return An Avro schema corresponding to the given result set's metadata
     * @throws IOException If an error occurs during schema discovery/building
     */
    public static Schema createSchema(final @NotNull Row row) throws IOException {
        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final int nrOfColumns = (columnDefinitions == null ? 0 : columnDefinitions.size());
        String tableName = "NiFi_Cassandra_Query_Record";

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName)
                .namespace("any.data")
                .fields();

        if (columnDefinitions != null) {
            for (int i = 0; i < nrOfColumns; i++) {
                DataType dataType = columnDefinitions.get(i).getType();
                if (dataType == null) {
                    throw new IllegalArgumentException("No data type for column[" + i + "] with name " +
                            columnDefinitions.get(i).getName().asInternal());
                }

                final String colName = columnDefinitions.get(i).getName().asInternal();

                if (dataType instanceof ListType) {
                    DataType elementType = ((ListType) dataType).getElementType();
                    builder.name(colName)
                            .type().unionOf()
                            .nullBuilder().endNull()
                            .and().array().items(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(elementType)))
                            .endUnion().noDefault();

                } else if (dataType instanceof SetType) {
                    DataType elementType = ((SetType) dataType).getElementType();
                    builder.name(colName)
                            .type().unionOf()
                            .nullBuilder().endNull()
                            .and().array().items(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(elementType)))
                            .endUnion().noDefault();

                } else if (dataType instanceof MapType) {
                    DataType valueType = ((MapType) dataType).getValueType();
                    builder.name(colName)
                            .type().unionOf()
                            .nullBuilder().endNull()
                            .and().map().values(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(valueType)))
                            .endUnion().noDefault();

                } else {
                    builder.name(colName)
                            .type(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(dataType)))
                            .noDefault();
                }
            }
        }
        return builder.endRecord();
    }
}
