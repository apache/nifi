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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.cassandra.CassandraClient;
import org.apache.nifi.cassandra.exception.CassandraException;
import org.apache.nifi.cassandra.exception.CassandraExceptionCategory;
import org.apache.nifi.cassandra.models.CassandraColumnDefinition;
import org.apache.nifi.cassandra.models.CassandraQueryRequest;
import org.apache.nifi.cassandra.models.CassandraQueryResult;
import org.apache.nifi.cassandra.models.CassandraRow;
import org.apache.nifi.cassandra.models.CassandraType;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.nifi.processors.cassandra.converter.CassandraTypeConverter;
import org.apache.nifi.processors.cassandra.converter.StandardCassandraTypeConverter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@Tags({"cassandra", "cql", "select"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("""
       Executes a targeted Cassandra Query Language (CQL) select query against a specified Cassandra cluster and
       writes the resulting data rows directly using the specifically configured Record Writer service.
       Streaming architecture is utilized so that even arbitrarily large result sets are fully supported.
       This processor can be scheduled to run on a timer or cron expression using standard NiFi scheduling methods,
       or it can be triggered by incoming FlowFile. If triggered by an incoming FlowFile, attributes of that FlowFile
       will be available when evaluating the select query.
       The attribute 'executecql.row.count' indicates the number of rows included in the resulting FlowFile.
       """)
@WritesAttributes({
        @WritesAttribute(
                attribute = "executecql.row.count",
                description = "The number of rows included in the FlowFile."
        ),
        @WritesAttribute(
                attribute = "fragment.identifier",
                description = """
                       If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set will have
                       the same value for fragment.identifier attribute. This can then be used to correlate the results.
                       """
        ),
        @WritesAttribute(
                attribute = "fragment.count",
                description = """
                       If 'Max Rows Per Flow File' is set then this is the total number of FlowFiles produced by
                       a single ResultSet. This can be used in conjunction with the fragment.identifier attribute
                       in order to know how many FlowFiles belonged to the same incoming ResultSet.
                       If Output Batch Size is set, then this attribute will not be populated.
                       """
        ),
        @WritesAttribute(
                attribute = "fragment.index",
                description = """
                       If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of outgoing
                       FlowFiles that were all derived from the same result set FlowFile.
                       This can be used in conjunction with the fragment.identifier attribute to know which FlowFiles
                       originated from the same query result set and in what order FlowFiles were produced.
                       """
        )
})
public class QueryCassandra extends AbstractCassandraProcessor {
    private static final CassandraTypeConverter typeConverter = new StandardCassandraTypeConverter();
    public static final String RESULT_ROW_COUNT = "executecql.row.count";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

    public static final PropertyDescriptor CQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("CQL Select Query")
            .description("CQL select query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("""
                   The maximum amount of time allowed for a running CQL select query.
                   A value of zero means driver default timeout will apply.
                   """)
            .defaultValue("0 seconds")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("""
                   The number of result rows to be fetched from the result set at a time.
                   Zero is the default and means there is no limit.
                   """)
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("Max Rows Per Flow File")
            .description("""
                   The maximum number of result rows that will be included in a single FlowFile. This will allow you
                   to break up very large result sets into multiple FlowFiles. If the value specified is zero,
                   then all rows are returned in a single FlowFile.
                   """)
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Output Batch Size")
            .description("""
                   The number of output FlowFiles to queue before committing the process session. When set to zero,
                   the session will be committed when all result set rows have been processed and the output FlowFiles
                   are ready for transfer to the downstream relationship.
                   If this property is set, then when the specified number of FlowFiles are ready for transfer,
                   then session will be committed, thus releasing the FlowFiles to the downstream relationship.
                   NOTE: The fragment.count attributes will not be set on FlowFiles when this property is set.
                   """)
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("""
                   Specifies the Record Writer controller service to be used for formatting the resulting query rows
                   into various formats such as JSON, CSV, Avro o XML, while setting the appropriate output content type
                   attribute on the outgoing FlowFiles based on the configured service.
                   """)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
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
                            RECORD_WRITER
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile incomingFlowFile = null;
        if (context.hasIncomingConnection()) {
            incomingFlowFile = session.get();
            if (incomingFlowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final String selectQuery = context.getProperty(CQL_SELECT_QUERY).evaluateAttributeExpressions(incomingFlowFile).getValue();
        final long queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(incomingFlowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        final ComponentLog logger = getLogger();
        final String transitUri = createTransitUri(getConnectionService(context), selectQuery);

        try {
            final CassandraClient cassandraClient = getConnectionService(context).getClient();

            final CassandraQueryRequest request = new CassandraQueryRequest(selectQuery,
                    fetchSize > 0 ? fetchSize : null,
                    queryTimeout > 0 ? Duration.ofMillis(queryTimeout) : null);

            final StopWatch stopWatch = new StopWatch(true);
            final CassandraQueryResult queryResult = cassandraClient.executeQuery(request);

            if (maxRowsPerFlowFile > 0) {
                writeSplitResultSet(session, writerFactory, incomingFlowFile, queryResult, maxRowsPerFlowFile,
                        outputBatchSize, transitUri, stopWatch);
            } else {
                writeSingleResultSet(session, writerFactory, incomingFlowFile, queryResult, transitUri, stopWatch);
            }
        } catch (final CassandraException ce) {
            logger.error("Cassandra query execution failed", ce);
            final Relationship relationship = (ce.getCategory() == CassandraExceptionCategory.RETRY) ? REL_RETRY : REL_FAILURE;
            routeOnError(context, session, incomingFlowFile, relationship);
        } catch (final ProcessException pe) {
            if (pe.getCause() instanceof CassandraException ce) {
                logger.error(ce.getMessage(), ce);
                final Relationship relationship = (ce.getCategory() == CassandraExceptionCategory.RETRY) ? REL_RETRY : REL_FAILURE;
                routeOnError(context, session, incomingFlowFile, relationship);
            } else {
                logger.error("Unable to execute CQL query", pe);
                routeOnError(context, session, incomingFlowFile, REL_FAILURE);
            }
        }
    }

    private RecordSchema buildRecordSchema(final List<CassandraColumnDefinition> columnDefinitions) {
        List<RecordField> fields = new ArrayList<>();

        if (columnDefinitions != null) {
            for (CassandraColumnDefinition columnDefinition : columnDefinitions) {
                String name = columnDefinition.name();
                CassandraType cassandraType = columnDefinition.type();
                org.apache.nifi.serialization.record.DataType nifiDataType = typeConverter.getDataType(cassandraType);
                fields.add(new RecordField(name, nifiDataType, true));
            }
        }
        return new SimpleRecordSchema(fields);
    }

    private void writeSingleResultSet(final ProcessSession session,
            final RecordSetWriterFactory writerFactory,
            final FlowFile incomingFlowFile,
            final CassandraQueryResult queryResult,
              final String transitUri,
              final StopWatch stopWatch) {

        final FlowFile flowFile = session.create();

        final AtomicLong rowCount = new AtomicLong(0);
        final AtomicReference<WriteResult> writeResultRef = new AtomicReference<>();
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>();

        final RecordSchema writeSchema = queryResult != null
                ? buildRecordSchema(queryResult.getColumnDefinitions())
                : null;

        FlowFile written;

        try {
            written = session.write(flowFile, out -> {

                try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, flowFile)) {

                    writer.beginRecordSet();
                    long total = 0;
                    CassandraQueryResult current = queryResult;

                    while (current != null) {

                        final List<CassandraColumnDefinition> defs = current.getColumnDefinitions();
                        final int colCount = defs.size();
                        final String[] cols = new String[colCount];
                        for (int i = 0; i < colCount; i++) {
                            cols[i] = defs.get(i).name();
                        }

                        for (CassandraRow row : current.getCurrentPage()) {
                            final Map<String, Object> values = new HashMap<>(colCount);
                            for (int i = 0; i < colCount; i++) {
                                values.put(cols[i],
                                        row.isNull(i)
                                                ? null
                                                : typeConverter.getCassandraObject(row, i));
                            }
                            writer.write(new MapRecord(writeSchema, values));
                            total++;
                        }

                        current = current.hasMorePages()
                                ? current.fetchNextPage()
                                : null;
                    }
                    rowCount.set(total);
                    writeResultRef.set(writer.finishRecordSet());
                    mimeTypeRef.set(writer.getMimeType());

                } catch (Exception e) {
                    throw new ProcessException(e);
                }
            });

        } catch (Exception e) {
            throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
        }
        written = applyAttributes(session, written, writeResultRef.get(), mimeTypeRef.get(), rowCount.get());
        session.getProvenanceReporter().receive(written, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(written, REL_SUCCESS);
        if (incomingFlowFile != null) {
            session.remove(incomingFlowFile);
        }
    }

    private void writeSplitResultSet(
            final ProcessSession session,
            final RecordSetWriterFactory writerFactory,
            final FlowFile incomingFlowFile,
            final CassandraQueryResult queryResult,
            final int maxRowsPerFlowFile,
            final int outputBatchSize,
            final String transitUri,
            final StopWatch stopWatch) {

        final List<FlowFile> allFlowFilesFromSet = new ArrayList<>();
        final List<FlowFile> batchResults = new ArrayList<>();
        final String fragmentId = UUID.randomUUID().toString();
        int fragmentIndex = 0;

        final RecordSchema writeSchema = buildRecordSchema(queryResult.getColumnDefinitions());
        final List<CassandraColumnDefinition> columnDefinitions = queryResult.getColumnDefinitions();
        final int colCount = columnDefinitions.size();
        final String[] cols = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            cols[i] = columnDefinitions.get(i).name();
        }

        final AtomicReference<CassandraQueryResult> currentResultSet = new AtomicReference<>(queryResult);
        final AtomicReference<java.util.Iterator<CassandraRow>> rowIterator = new AtomicReference<>(
                currentResultSet.get().getCurrentPage().iterator());

        if (incomingFlowFile != null) {
            session.remove(incomingFlowFile);
        }

        try {
            while (rowIterator.get().hasNext() || currentResultSet.get().hasMorePages()) {

                final FlowFile flowFile = session.create();
                final AtomicLong rowsInThisFile = new AtomicLong(0);
                final AtomicReference<WriteResult> writeResultRef = new AtomicReference<>();
                final AtomicReference<String> mimeTypeRef = new AtomicReference<>();

                FlowFile written = session.write(flowFile, out -> {
                    try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, flowFile)) {
                        writer.beginRecordSet();
                        int localCount = 0;

                        while (localCount < maxRowsPerFlowFile) {
                            if (!rowIterator.get().hasNext() && currentResultSet.get().hasMorePages()) {
                                currentResultSet.set(currentResultSet.get().fetchNextPage());
                                rowIterator.set(currentResultSet.get().getCurrentPage().iterator());
                            }

                            if (rowIterator.get().hasNext()) {
                                final CassandraRow row = rowIterator.get().next();
                                final Map<String, Object> values = new HashMap<>(colCount);
                                for (int i = 0; i < colCount; i++) {
                                    values.put(cols[i],
                                            row.isNull(i) ? null : typeConverter.getCassandraObject(row, i));
                                }
                                writer.write(new MapRecord(writeSchema, values));
                                localCount++;
                            } else {
                                break;
                            }
                        }
                        rowsInThisFile.set(localCount);
                        writeResultRef.set(writer.finishRecordSet());
                        mimeTypeRef.set(writer.getMimeType());
                    } catch (Exception e) {
                        throw new ProcessException(e);
                    }
                });

                if (rowsInThisFile.get() == 0) {
                    session.remove(written);
                    break;
                }

                written = applyAttributes(session, written, writeResultRef.get(), mimeTypeRef.get(),
                        rowsInThisFile.get());
                written = session.putAttribute(written, FRAGMENT_ID, fragmentId);
                written = session.putAttribute(written, FRAGMENT_INDEX, String.valueOf(fragmentIndex++));

                session.getProvenanceReporter().receive(written, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));

                if (outputBatchSize == 0) {
                    allFlowFilesFromSet.add(written);
                }

                batchResults.add(written);

                if (outputBatchSize > 0 && batchResults.size() >= outputBatchSize) {
                    session.transfer(batchResults, REL_SUCCESS);
                    session.commitAsync();
                    batchResults.clear();
                }
            }

            if (outputBatchSize == 0 && !allFlowFilesFromSet.isEmpty()) {
                final String totalCount = String.valueOf(allFlowFilesFromSet.size());
                allFlowFilesFromSet.replaceAll(flowFile -> session.putAttribute(flowFile, FRAGMENT_COUNT, totalCount));
            }

            if (!batchResults.isEmpty()) {
                session.transfer(batchResults, REL_SUCCESS);
            }

        } catch (Exception e) {
            throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
        }
    }

    private FlowFile applyAttributes(ProcessSession session,
            FlowFile flowFile,
            WriteResult result,
            String mimeType,
            long rowCount) {

        if (result != null && !result.getAttributes().isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, result.getAttributes());
        }

        if (mimeType != null) {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType);
        }

        return session.putAttribute(flowFile, RESULT_ROW_COUNT, String.valueOf(rowCount));
    }
}
