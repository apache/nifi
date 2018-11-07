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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"cassandra", "cql", "select"})
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Execute provided Cassandra Query Language (CQL) select query on a Cassandra 1.x, 2.x, or 3.0.x cluster. Query result "
        + "may be converted to Avro or JSON format. Streaming is used so arbitrarily large result sets are supported. This processor can be "
        + "scheduled to run on a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'executecql.row.count' indicates how many rows were selected.")
@WritesAttributes({@WritesAttribute(attribute = "executecql.row.count", description = "The number of rows returned by the CQL query")})
public class QueryCassandra extends AbstractCassandraProcessor {

    public static final String AVRO_FORMAT = "Avro";
    public static final String JSON_FORMAT = "JSON";

    public static final String RESULT_ROW_COUNT = "executecql.row.count";

    public static final PropertyDescriptor CQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("CQL select query")
            .description("CQL select query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running CQL select query. Must be of format "
                    + "<duration> <TimeUnit> where <duration> is a non-negative integer and TimeUnit is a supported "
                    + "Time Unit, such as: nanos, millis, secs, mins, hrs, days. A value of zero means there is no limit. ")
            .defaultValue("0 seconds")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch size")
            .description("The number of result rows to be fetched from the result set at a time. Zero is the default "
                    + "and means there is no limit.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
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

    private final static List<PropertyDescriptor> propertyDescriptors;

    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(CQL_SELECT_QUERY);
        _propertyDescriptors.add(QUERY_TIMEOUT);
        _propertyDescriptors.add(FETCH_SIZE);
        _propertyDescriptors.add(MAX_ROWS_PER_FLOW_FILE);
        _propertyDescriptors.add(OUTPUT_FORMAT);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_ORIGINAL);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
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
        ComponentLog log = getLogger();
        try {
            connectToCassandra(context);
            final int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
            if (fetchSize > 0) {
                synchronized (cluster.get()) {
                    cluster.get().getConfiguration().getQueryOptions().setFetchSize(fetchSize);
                }
            }
        } catch (final NoHostAvailableException nhae) {
            log.error("No host in the Cassandra cluster can be contacted successfully to execute this query", nhae);
            // Log up to 10 error messages. Otherwise if a 1000-node cluster was specified but there was no connectivity,
            // a thousand error messages would be logged. However we would like information from Cassandra itself, so
            // cap the error limit at 10, format the messages, and don't include the stack trace (it is displayed by the
            // logger message above).
            log.error(nhae.getCustomMessage(10, true, false));
            throw new ProcessException(nhae);
        } catch (final AuthenticationException ae) {
            log.error("Invalid username/password combination", ae);
            throw new ProcessException(ae);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = null;
        FlowFile fileToProcess = null;

        Map<String, String> attributes = null;

        if (context.hasIncomingConnection()) {
            inputFlowFile = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (inputFlowFile == null && context.hasNonLoopConnection()) {
                return;
            }

            attributes = inputFlowFile.getAttributes();
        }

        final ComponentLog logger = getLogger();
        final String selectQuery = context.getProperty(CQL_SELECT_QUERY).evaluateAttributeExpressions(inputFlowFile).getValue();
        final long queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(inputFlowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final String outputFormat = context.getProperty(OUTPUT_FORMAT).getValue();
        final long maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(inputFlowFile).getValue());
        final StopWatch stopWatch = new StopWatch(true);

        if(inputFlowFile != null){
            session.transfer(inputFlowFile, REL_ORIGINAL);
        }

        try {
            // The documentation for the driver recommends the session remain open the entire time the processor is running
            // and states that it is thread-safe. This is why connectionSession is not in a try-with-resources.
            final Session connectionSession = cassandraSession.get();
            final ResultSet resultSet;

            if (queryTimeout > 0) {
                resultSet = connectionSession.execute(selectQuery, queryTimeout, TimeUnit.MILLISECONDS);
            }else{
                resultSet = connectionSession.execute(selectQuery);
            }

            final AtomicLong nrOfRows = new AtomicLong(0L);

            do {
                fileToProcess = session.create();

                // Assuring that if we have an input FlowFile
                // the generated output inherit the attributes
                if(attributes != null){
                    fileToProcess = session.putAllAttributes(fileToProcess, attributes);
                }

                fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        try {
                            logger.debug("Executing CQL query {}", new Object[]{selectQuery});
                            if (queryTimeout > 0) {
                                if (AVRO_FORMAT.equals(outputFormat)) {
                                    nrOfRows.set(convertToAvroStream(resultSet, maxRowsPerFlowFile,
                                            out, queryTimeout, TimeUnit.MILLISECONDS));
                                } else if (JSON_FORMAT.equals(outputFormat)) {
                                    nrOfRows.set(convertToJsonStream(resultSet, maxRowsPerFlowFile,
                                            out, charset, queryTimeout, TimeUnit.MILLISECONDS));
                                }
                            } else {
                                if (AVRO_FORMAT.equals(outputFormat)) {
                                    nrOfRows.set(convertToAvroStream(resultSet, maxRowsPerFlowFile,
                                            out, 0, null));
                                } else if (JSON_FORMAT.equals(outputFormat)) {
                                    nrOfRows.set(convertToJsonStream(resultSet, maxRowsPerFlowFile,
                                            out, charset, 0, null));
                                }
                            }
                        } catch (final TimeoutException | InterruptedException | ExecutionException e) {
                            throw new ProcessException(e);
                        }
                    }
                });

                // set attribute how many rows were selected
                fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));

                // set mime.type based on output format
                fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(),
                        JSON_FORMAT.equals(outputFormat) ? "application/json" : "application/avro-binary");

                logger.info("{} contains {} records; transferring to 'success'",
                        new Object[]{fileToProcess, nrOfRows.get()});
                session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + nrOfRows.get() + " rows",
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(fileToProcess, REL_SUCCESS);
                session.commit();

                try {
                    resultSet.fetchMoreResults().get();
                } catch (Exception e) {
                    logger.error("ExecutionException : query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, fileToProcess, e});
                }
            } while (!resultSet.isExhausted());

        } catch (final NoHostAvailableException nhae) {
            getLogger().error("No host in the Cassandra cluster can be contacted successfully to execute this query", nhae);
            // Log up to 10 error messages. Otherwise if a 1000-node cluster was specified but there was no connectivity,
            // a thousand error messages would be logged. However we would like information from Cassandra itself, so
            // cap the error limit at 10, format the messages, and don't include the stack trace (it is displayed by the
            // logger message above).
            getLogger().error(nhae.getCustomMessage(10, true, false));
            if (fileToProcess == null) {
                fileToProcess = session.create();
            }
            fileToProcess = session.penalize(fileToProcess);
            session.transfer(fileToProcess, REL_RETRY);
        } catch (final QueryExecutionException qee) {
            //session.rollback();
            logger.error("Cannot execute the query with the requested consistency level successfully", qee);
            if (fileToProcess == null) {
                fileToProcess = session.create();
            }
            fileToProcess = session.penalize(fileToProcess);
            session.transfer(fileToProcess, REL_RETRY);

        } catch (final QueryValidationException qve) {
            if (context.hasIncomingConnection()) {
                logger.error("The CQL query {} is invalid due to syntax error, authorization issue, or another "
                                + "validation problem; routing {} to failure",
                        new Object[]{selectQuery, fileToProcess}, qve);

                if (fileToProcess == null) {
                    fileToProcess = session.create();
                }
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_FAILURE);
            } else {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("The CQL query {} is invalid due to syntax error, authorization issue, or another "
                        + "validation problem", new Object[]{selectQuery}, qve);
                if (fileToProcess != null) {
                    session.remove(fileToProcess);
                }
                context.yield();
            }
        } catch (final ProcessException e) {
            if (context.hasIncomingConnection()) {
                logger.error("Unable to execute CQL select query {} for {} due to {}; routing to failure",
                        new Object[]{selectQuery, fileToProcess, e});
                if (fileToProcess == null) {
                    fileToProcess = session.create();
                }
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_FAILURE);

            } else {
                logger.error("Unable to execute CQL select query {} due to {}",
                        new Object[]{selectQuery, e});
                if (fileToProcess != null) {
                    session.remove(fileToProcess);
                }
                context.yield();
            }
        }
        session.commit();
    }


    @OnUnscheduled
    public void stop() {
        super.stop();
    }

    @OnShutdown
    public void shutdown() {
        super.stop();
    }

    /**
     * Converts a result set into an Avro record and writes it to the given stream.
     *
     * @param rs        The result set to convert
     * @param outStream The stream to which the Avro record will be written
     * @param timeout   The max number of timeUnits to wait for a result set fetch to complete
     * @param timeUnit  The unit of time (SECONDS, e.g.) associated with the timeout amount
     * @return The number of rows from the result set written to the stream
     * @throws IOException          If the Avro record cannot be written
     * @throws InterruptedException If a result set fetch is interrupted
     * @throws TimeoutException     If a result set fetch has taken longer than the specified timeout
     * @throws ExecutionException   If any error occurs during the result set fetch
     */
    public static long convertToAvroStream(final ResultSet rs, long maxRowsPerFlowFile,
                                           final OutputStream outStream,
                                           long timeout, TimeUnit timeUnit)
            throws IOException, InterruptedException, TimeoutException, ExecutionException {

        final Schema schema = createSchema(rs);
        final GenericRecord rec = new GenericData.Record(schema);
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
            long nrOfRows = 0;
            long rowsAvailableWithoutFetching = rs.getAvailableWithoutFetching();

            if (columnDefinitions != null) {

                // Grab the ones we have
                if (rowsAvailableWithoutFetching == 0
                        || rowsAvailableWithoutFetching < maxRowsPerFlowFile) {
                    // Get more
                    if (timeout <= 0 || timeUnit == null) {
                        rs.fetchMoreResults().get();
                    } else {
                        rs.fetchMoreResults().get(timeout, timeUnit);
                    }
                    rowsAvailableWithoutFetching = rs.getAvailableWithoutFetching();
                }

                if(maxRowsPerFlowFile == 0){
                    maxRowsPerFlowFile = rowsAvailableWithoutFetching;
                }

                Row row;
                //Iterator<Row> it = rs.iterator();
                while(nrOfRows < maxRowsPerFlowFile){
                    try {
                        row = rs.iterator().next();
                    }catch (NoSuchElementException nsee){
                        nrOfRows -= 1;
                        break;
                    }

                    // iterator().next() is like iterator().one() => return null on end
                    // https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/ResultSet.html#one--
                    if(row == null){
                        break;
                    }

                    for (int i = 0; i < columnDefinitions.size(); i++) {
                        final DataType dataType = columnDefinitions.getType(i);

                        if (row.isNull(i)) {
                            rec.put(i, null);
                        } else {
                            rec.put(i, getCassandraObject(row, i, dataType));
                        }
                    }

                    dataFileWriter.append(rec);
                    nrOfRows += 1;
                }
            }
            return nrOfRows;
        }
    }

    /**
     * Converts a result set into an Json object and writes it to the given stream using the specified character set.
     *
     * @param rs        The result set to convert
     * @param outStream The stream to which the JSON object will be written
     * @param timeout   The max number of timeUnits to wait for a result set fetch to complete
     * @param timeUnit  The unit of time (SECONDS, e.g.) associated with the timeout amount
     * @return The number of rows from the result set written to the stream
     * @throws IOException          If the JSON object cannot be written
     * @throws InterruptedException If a result set fetch is interrupted
     * @throws TimeoutException     If a result set fetch has taken longer than the specified timeout
     * @throws ExecutionException   If any error occurs during the result set fetch
     */
    public static long convertToJsonStream(final ResultSet rs, long maxRowsPerFlowFile,
                                           final OutputStream outStream,
                                           Charset charset, long timeout, TimeUnit timeUnit)
            throws IOException, InterruptedException, TimeoutException, ExecutionException {

        try {
            // Write the initial object brace
            outStream.write("{\"results\":[".getBytes(charset));
            ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
            long nrOfRows = 0;
            long rowsAvailableWithoutFetching = rs.getAvailableWithoutFetching();

            if (columnDefinitions != null) {

                // Grab the ones we have
                if (rowsAvailableWithoutFetching == 0) {
                    // Get more
                    if (timeout <= 0 || timeUnit == null) {
                        rs.fetchMoreResults().get();
                    } else {
                        rs.fetchMoreResults().get(timeout, timeUnit);
                    }
                    rowsAvailableWithoutFetching = rs.getAvailableWithoutFetching();
                }

                if(maxRowsPerFlowFile == 0){
                    maxRowsPerFlowFile = rowsAvailableWithoutFetching;
                }

                Row row;
                while(nrOfRows < maxRowsPerFlowFile){
                    try {
                        row = rs.iterator().next();
                    }catch (NoSuchElementException nsee){
                        nrOfRows -= 1;
                        break;
                    }

                    // iterator().next() is like iterator().one() => return null on end
                    // https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/ResultSet.html#one--
                    if(row == null){
                        break;
                    }

                    if (nrOfRows != 0) {
                        outStream.write(",".getBytes(charset));
                    }

                    outStream.write("{".getBytes(charset));
                    for (int i = 0; i < columnDefinitions.size(); i++) {
                        final DataType dataType = columnDefinitions.getType(i);
                        final String colName = columnDefinitions.getName(i);
                        if (i != 0) {
                            outStream.write(",".getBytes(charset));
                        }
                        if (row.isNull(i)) {
                            outStream.write(("\"" + colName + "\"" + ":null").getBytes(charset));
                        } else {
                            Object value = getCassandraObject(row, i, dataType);
                            String valueString;
                            if (value instanceof List || value instanceof Set) {
                                boolean first = true;
                                StringBuilder sb = new StringBuilder("[");
                                for (Object element : ((Collection) value)) {
                                    if (!first) {
                                        sb.append(",");
                                    }
                                    sb.append(getJsonElement(element));
                                    first = false;
                                }
                                sb.append("]");
                                valueString = sb.toString();
                            } else if (value instanceof Map) {
                                boolean first = true;
                                StringBuilder sb = new StringBuilder("{");
                                for (Object element : ((Map) value).entrySet()) {
                                    Map.Entry entry = (Map.Entry) element;
                                    Object mapKey = entry.getKey();
                                    Object mapValue = entry.getValue();

                                    if (!first) {
                                        sb.append(",");
                                    }
                                    sb.append(getJsonElement(mapKey));
                                    sb.append(":");
                                    sb.append(getJsonElement(mapValue));
                                    first = false;
                                }
                                sb.append("}");
                                valueString = sb.toString();
                            } else {
                                valueString = getJsonElement(value);
                            }
                            outStream.write(("\"" + colName + "\":"
                                    + valueString + "").getBytes(charset));
                        }
                    }
                    nrOfRows += 1;
                    outStream.write("}".getBytes(charset));
                }
            }
            return nrOfRows;
        } finally {
            outStream.write("]}".getBytes());
        }
    }

    protected static String getJsonElement(Object value) {
        if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return "\"" + dateFormat.format((Date) value) + "\"";
        } else if (value instanceof String) {
            return "\"" + StringEscapeUtils.escapeJson((String) value) + "\"";
        } else {
            return "\"" + value.toString() + "\"";
        }
    }

    /**
     * Creates an Avro schema from the given result set. The metadata (column definitions, data types, etc.) is used
     * to determine a schema for Avro.
     *
     * @param rs The result set from which an Avro schema will be created
     * @return An Avro schema corresponding to the given result set's metadata
     * @throws IOException If an error occurs during schema discovery/building
     */
    public static Schema createSchema(final ResultSet rs) throws IOException {
        final ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
        final int nrOfColumns = (columnDefinitions == null ? 0 : columnDefinitions.size());
        String tableName = "NiFi_Cassandra_Query_Record";
        if (nrOfColumns > 0) {
            String tableNameFromMeta = columnDefinitions.getTable(0);
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
        if (columnDefinitions != null) {
            for (int i = 0; i < nrOfColumns; i++) {

                DataType dataType = columnDefinitions.getType(i);
                if (dataType == null) {
                    throw new IllegalArgumentException("No data type for column[" + i + "] with name " + columnDefinitions.getName(i));
                }

                // Map types from Cassandra to Avro where possible
                if (dataType.isCollection()) {
                    List<DataType> typeArguments = dataType.getTypeArguments();
                    if (typeArguments == null || typeArguments.size() == 0) {
                        throw new IllegalArgumentException("Column[" + i + "] " + dataType.getName()
                                + " is a collection but no type arguments were specified!");
                    }
                    // Get the first type argument, to be used for lists and sets
                    DataType firstArg = typeArguments.get(0);
                    if (dataType.equals(DataType.set(firstArg))
                            || dataType.equals(DataType.list(firstArg))) {
                        builder.name(columnDefinitions.getName(i)).type().unionOf().nullBuilder().endNull().and().array()
                                .items(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(firstArg))).endUnion().noDefault();
                    } else {
                        // Must be an n-arg collection like map
                        DataType secondArg = typeArguments.get(1);
                        if (dataType.equals(DataType.map(firstArg, secondArg))) {
                            builder.name(columnDefinitions.getName(i)).type().unionOf().nullBuilder().endNull().and().map().values(
                                    getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(secondArg))).endUnion().noDefault();
                        }
                    }
                } else {
                    builder.name(columnDefinitions.getName(i))
                            .type(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(dataType))).noDefault();
                }
            }
        }
        return builder.endRecord();
    }
}

