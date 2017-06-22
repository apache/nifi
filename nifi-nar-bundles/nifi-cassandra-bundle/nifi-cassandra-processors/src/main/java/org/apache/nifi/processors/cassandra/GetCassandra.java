package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.*;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StopWatch;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


@Tags({"cassandra", "cql", "select"})
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Executes provided Cassandra Query Language (CQL) select query on a Cassandra to fetch all rows whose values"
        + "in the specified Maximum Value column(s) are larger than the previously-seen maxima.Query result"
        + "may be converted to Avro, JSON or CSV format. Streaming is used so arbitrarily large result sets are supported. This processor can be "
        + "scheduled to run on a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query. FlowFile attribute 'executecql.row.count' indicates how many rows were selected.")
@WritesAttributes({@WritesAttribute(attribute = "executecql.row.count", description = "The number of rows returned by the CQL query")})
public class GetCassandra extends QueryCassandra {

    private static final long INIT_WATER_MARK_VALUE = -1;
    private static final String CSV_FORMAT = "CSV";
    private static final int DEFAULT_CASSANDRA_PORT = 9042;
    private static final String RESULT_ROW_COUNT = "executecql.row.count";
    private static final CodecRegistry codecRegistry = new CodecRegistry();
    
    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;
    private final AtomicReference<Cluster> cluster = new AtomicReference<>(null);
    private final AtomicReference<Session> cassandraSession = new AtomicReference<>(null);

    /**
     * PropertyDescriptor
     */
    public static final PropertyDescriptor DATE_FIELD = new PropertyDescriptor.Builder()
            .name("Date Field")
            .description("Source field containing a modified date for tracking incremental load")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("Load for Table Name")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * static
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);

        _propertyDescriptors.add(DATE_FIELD);
        _propertyDescriptors.add(TABLE_NAME);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Ensure that if username or password is set, then the other is too
        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        if (StringUtils.isEmpty(propertyMap.get(USERNAME)) != StringUtils.isEmpty(propertyMap.get(PASSWORD))) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                                                                                "If username or password is specified, then the other must be specified as well")
                            .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final String selectQuery = context.getProperty(CQL_SELECT_QUERY).getValue();
        final long queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final String outputFormat = context.getProperty(OUTPUT_FORMAT).getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final StopWatch stopWatch = new StopWatch(true);
        final String waterMarkDateField = context.getProperty(DATE_FIELD).getValue();
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final String keySpace = context.getProperty(KEYSPACE).getValue();

        if ((selectQuery == null || StringUtils.isEmpty(selectQuery)) && (tableName == null || StringUtils.isEmpty(tableName))) {
            throw new ProcessException(" TableName or Query is required");
        }

        long minBoundValue = INIT_WATER_MARK_VALUE;
        long maxBoundValue = INIT_WATER_MARK_VALUE;

        if (fileToProcess != null) {
            String minValue = fileToProcess.getAttribute(WaterMarkConst.CASSANDRA_WATERMARK_MIN_VALUE_ID);
            String maxValue = fileToProcess.getAttribute(WaterMarkConst.CASSANDRA_WATERMARK_MAX_VALUE_ID);
            if (minValue != null && !StringUtils.isEmpty(minValue)) {
                minBoundValue = Long.valueOf(minValue);
            }
            if (maxValue != null && !StringUtils.isEmpty(maxValue)) {
                maxBoundValue = Long.valueOf(maxValue);
            }
        }else{
            fileToProcess = session.create();
        }

        String finalSelectQuery = queryMake(tableName, keySpace, waterMarkDateField, selectQuery, minBoundValue, maxBoundValue);

        logger.debug("===========================================================================");
        logger.debug("= GetCassandra ");
        logger.debug("===========================================================================");
        logger.debug("tableName            : " + tableName);
        logger.debug("keySpace             : " + keySpace);
        logger.debug("waterMarkDateField   : " + waterMarkDateField);
        logger.debug("selectQuery          : " + selectQuery);
        logger.debug("minBoundValue        : " + minBoundValue);
        logger.debug("maxBoundValue        : " + maxBoundValue);
        logger.debug("finalSelectQuery     : " + finalSelectQuery);
        logger.debug("===========================================================================");

        try {
            final Session connectionSession = cassandraSession.get();
            final ResultSetFuture queryFuture = connectionSession.executeAsync(finalSelectQuery);
            final AtomicLong nrOfRows = new AtomicLong(0L);

            fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try {

                        final ResultSet resultSet;
                        if (queryTimeout > 0) {
                            resultSet = queryFuture.getUninterruptibly(queryTimeout, TimeUnit.MILLISECONDS);
                            if (AVRO_FORMAT.equals(outputFormat)) {
                                nrOfRows.set(convertToAvroStream(resultSet, out, queryTimeout, TimeUnit.MILLISECONDS));
                            } else if (JSON_FORMAT.equals(outputFormat)) {
                                nrOfRows.set(convertToJsonStream(resultSet, out, charset, queryTimeout, TimeUnit.MILLISECONDS));
                            } else if (CSV_FORMAT.equals(outputFormat)) {
                                nrOfRows.set(convertToCSVStream(resultSet, out, charset, queryTimeout, TimeUnit.MILLISECONDS));
                            }
                        } else {
                            resultSet = queryFuture.getUninterruptibly();
                            if (AVRO_FORMAT.equals(outputFormat)) {
                                nrOfRows.set(convertToAvroStream(resultSet, out, 0, null));
                            } else if (JSON_FORMAT.equals(outputFormat)) {
                                nrOfRows.set(convertToJsonStream(resultSet, out, charset, 0, null));
                            } else if (CSV_FORMAT.equals(outputFormat)) {
                                nrOfRows.set(convertToCSVStream(resultSet, out, charset, 0, null));
                            }
                        }

                    } catch (final TimeoutException | InterruptedException | ExecutionException e) {
                        throw new ProcessException(e);
                    }
                }
            });

            // set attribute how many rows were selected
            fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));

            // logger.info("{} contains {} records; transferring to 'success'", new Object[]
            // {fileToProcess, nrOfRows.get()});
            logger.debug("==================================================");
            logger.debug(" " + nrOfRows.get() + " records read");
            logger.debug("==================================================");
            session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + nrOfRows.get() + " rows",
                                                          stopWatch.getElapsed(TimeUnit.MILLISECONDS));

            session.transfer(fileToProcess, REL_SUCCESS);

        } catch (final NoHostAvailableException nhae) {
            getLogger().error("No host in the Cassandra cluster can be contacted successfully to execute this query", nhae);
            // Log up to 10 error messages. Otherwise if a 1000-node cluster was specified but there
            // was no connectivity,
            // a thousand error messages would be logged. However we would like information from
            // Cassandra itself, so
            // cap the error limit at 10, format the messages, and don't include the stack trace (it
            // is displayed by the
            // logger message above).
            getLogger().error(nhae.getCustomMessage(10, true, false));
            fileToProcess = session.penalize(fileToProcess);
            session.transfer(fileToProcess, REL_RETRY);

        } catch (final QueryExecutionException qee) {
            logger.error("Cannot execute the query with the requested consistency level successfully", qee);
            fileToProcess = session.penalize(fileToProcess);
            session.transfer(fileToProcess, REL_RETRY);

        } catch (final QueryValidationException qve) {
            if (context.hasIncomingConnection()) {
                logger.error("The CQL query {} is invalid due to syntax error, authorization issue, or another "
                             + "validation problem; routing {} to failure",
                             new Object[] {finalSelectQuery, fileToProcess}, qve);
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_FAILURE);
            } else {
                // This can happen if any exceptions occur while setting up the connection,
                // statement, etc.
                logger.error("The CQL query {} is invalid due to syntax error, authorization issue, or another "
                             + "validation problem", new Object[] {finalSelectQuery}, qve);
                session.remove(fileToProcess);
                context.yield();
            }
        } catch (final ProcessException e) {
            if (context.hasIncomingConnection()) {
                logger.error("Unable to execute CQL select query {} for {} due to {}; routing to failure",
                             new Object[] {finalSelectQuery, fileToProcess, e});
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_FAILURE);
            } else {
                logger.error("Unable to execute CQL select query {} due to {}",
                             new Object[] {finalSelectQuery, e});
                session.remove(fileToProcess);
                context.yield();
            }
        }
    }

    private String queryMake(String tableName, String keySpace, String waterMarkDateField, String selectQuery, long minBoundValue, long maxBoundValue) {

        // Query Make
        String finalSelectQuery;
        // based on table
        if (!StringUtils.isEmpty(tableName)) {
            StringBuffer sb = new StringBuffer();
            // incremental data
            if (waterMarkDateField != null && !StringUtils.isEmpty(waterMarkDateField) && minBoundValue != INIT_WATER_MARK_VALUE && maxBoundValue != INIT_WATER_MARK_VALUE) {
                sb.append("select * from ");
                sb.append(keySpace);
                sb.append(".");
                sb.append(tableName);
                sb.append(" where ");
                sb.append(waterMarkDateField);
                sb.append(" >= ");
                sb.append(minBoundValue);
                sb.append(" and ");
                sb.append(waterMarkDateField);
                sb.append(" <= ");
                sb.append(maxBoundValue);
                sb.append(" allow filtering");
            }
            // full data
            else {
                sb.append("select * from ");
                sb.append(keySpace);
                sb.append(".");
                sb.append(tableName);
            }
            finalSelectQuery = sb.toString();
        }
        // based on query
        else {
            // incremental data
            if (waterMarkDateField != null && !StringUtils.isEmpty(waterMarkDateField) && minBoundValue != INIT_WATER_MARK_VALUE && maxBoundValue != INIT_WATER_MARK_VALUE) {
                String sql = selectQuery.toLowerCase().replaceAll("allow filtering", "");
                StringBuffer sb = new StringBuffer();
                if (sql.indexOf("where") > 0) {
                    sb.append(sql);
                    sb.append(" and ");
                    sb.append(waterMarkDateField);
                    sb.append(" >= ");
                    sb.append(minBoundValue);
                    sb.append(" and ");
                    sb.append(waterMarkDateField);
                    sb.append(" <= ");
                    sb.append(maxBoundValue);
                    sb.append(" allow filtering");

                } else {
                    sb.append(sql);
                    sb.append(" where ");
                    sb.append(waterMarkDateField);
                    sb.append(" >= ");
                    sb.append(minBoundValue);
                    sb.append(" and ");
                    sb.append(waterMarkDateField);
                    sb.append(" <= ");
                    sb.append(maxBoundValue);
                    sb.append(" allow filtering");
                }
                finalSelectQuery = sb.toString();
            }
            // full data
            else {
                finalSelectQuery = selectQuery;
            }
        }

        return finalSelectQuery;
    }

    public static long convertToCSVStream(final ResultSet rs, final OutputStream outStream, Charset charset,
                                          long timeout, TimeUnit timeUnit)
                                                                           throws IOException, InterruptedException, TimeoutException, ExecutionException {

        try {
            // Write the initial object brace

            final ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();

            long nrOfRows = 0;

            do {

                // Grab the ones we have
                int rowsAvailableWithoutFetching = rs.getAvailableWithoutFetching();
                if (rowsAvailableWithoutFetching == 0) {
                    // Get more
                    if (timeout <= 0 || timeUnit == null) {
                        rs.fetchMoreResults().get();
                    } else {
                        rs.fetchMoreResults().get(timeout, timeUnit);
                    }
                }

                for (Row row : rs) {
                    if (nrOfRows != 0) {
                        outStream.write("\n".getBytes(charset));
                    }

                    for (int i = 0; i < columnDefinitions.size(); i++) {
                        final DataType dataType = columnDefinitions.getType(i);                        
                        if (i != 0) {
                            outStream.write(",".getBytes(charset));
                        }

                        if (row.isNull(i)) {
                            // skip data
                        } else {
                            Object value = getCassandraObject(row, i, dataType);
                            String valueString = value.toString();
                            outStream.write(valueString.getBytes(charset));

                        }
                    }
                    nrOfRows += 1;
                }
            } while (!rs.isFullyFetched());

            return nrOfRows;
        } 
        finally {            
        }
    }
}
