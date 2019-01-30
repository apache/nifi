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

package org.apache.nifi.processors.kudu;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "database", "NoSQL", "kudu", "HDFS", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to the specified Kudu's table. The schema for the table must be provided in the processor properties or from your source." +
        " If any error occurs while reading records from the input, or writing records to Kudu, the FlowFile will be routed to failure")
@WritesAttribute(attribute = "record.count", description = "Number of records written to Kudu")
public class PutKudu extends AbstractProcessor {
    protected static final PropertyDescriptor KUDU_MASTERS = new PropertyDescriptor.Builder()
        .name("Kudu Masters")
        .description("List all kudu masters's ip with port (e.g. 7051), comma separated")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
        .name("Table Name")
        .description("The name of the Kudu Table to put data into")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("The service for reading records from incoming flow files.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    protected static final PropertyDescriptor SKIP_HEAD_LINE = new PropertyDescriptor.Builder()
        .name("Skip head line")
        .description("Deprecated. Used to ignore header lines, but this should be handled by a RecordReader " +
            "(e.g. \"Treat First Line as Header\" property of CSVReader)")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor INSERT_OPERATION = new PropertyDescriptor.Builder()
        .name("Insert Operation")
        .description("Specify operationType for this processor. Insert-Ignore will ignore duplicated rows")
        .allowableValues(OperationType.values())
        .defaultValue(OperationType.INSERT.toString())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor FLUSH_MODE = new PropertyDescriptor.Builder()
        .name("Flush Mode")
        .description("Set the new flush mode for a kudu session.\n" +
            "AUTO_FLUSH_SYNC: the call returns when the operation is persisted, else it throws an exception.\n" +
            "AUTO_FLUSH_BACKGROUND: the call returns when the operation has been added to the buffer. This call should normally perform only fast in-memory" +
            " operations but it may have to wait when the buffer is full and there's another buffer being flushed.\n" +
            "MANUAL_FLUSH: the call returns when the operation has been added to the buffer, else it throws a KuduException if the buffer is full.")
        .allowableValues(SessionConfiguration.FlushMode.values())
        .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND.toString())
        .required(true)
        .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. " +
            "Depending on your memory size, and data size per row set an appropriate batch size. " +
            "Gradually increase this number to find out the best one for best performances.")
        .defaultValue("100")
        .required(true)
        .addValidator(StandardValidators.createLongValidator(1, 100000, true))
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship after it has been successfully stored in Kudu")
        .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if it cannot be sent to Kudu")
        .build();

    public static final String RECORD_COUNT_ATTR = "record.count";

    protected OperationType operationType;
    protected SessionConfiguration.FlushMode flushMode;
    protected int batchSize = 100;

    protected KuduClient kuduClient;
    protected KuduTable kuduTable;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(TABLE_NAME);
        properties.add(SKIP_HEAD_LINE);
        properties.add(RECORD_READER);
        properties.add(INSERT_OPERATION);
        properties.add(FLUSH_MODE);
        properties.add(BATCH_SIZE);

        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }


    @OnScheduled
    public void OnScheduled(final ProcessContext context) throws KuduException {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final String kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        operationType = OperationType.valueOf(context.getProperty(INSERT_OPERATION).getValue());
        batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        flushMode = SessionConfiguration.FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue());

        getLogger().debug("Setting up Kudu connection...");
        kuduClient = createClient(kuduMasters);
        kuduTable = kuduClient.openTable(tableName);
        getLogger().debug("Kudu connection successfully initialized");
    }

    protected KuduClient createClient(final String masters) {
        return new KuduClient.KuduClientBuilder(masters).build();
    }

    @OnStopped
    public final void closeClient() throws KuduException {
        if (kuduClient != null) {
            getLogger().debug("Closing KuduClient");
            kuduClient.close();
            kuduClient = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final KuduSession kuduSession = getKuduSession(kuduClient);
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final Map<FlowFile, Integer> numRecords = new HashMap<>();
        final Map<FlowFile, Object> flowFileFailures = new HashMap<>();
        final Map<Operation, FlowFile> operationFlowFileMap = new HashMap<>();

        int numBuffered = 0;
        final List<RowError> pendingRowErrors = new ArrayList<>();
        for (FlowFile flowFile : flowFiles) {
            try (final InputStream in = session.read(flowFile);
                 final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger())) {
                final List<String> fieldNames = recordReader.getSchema().getFieldNames();
                final RecordSet recordSet = recordReader.createRecordSet();

                Record record = recordSet.next();
                while (record != null) {
                    Operation operation = operationType == OperationType.UPSERT
                        ? upsertRecordToKudu(kuduTable, record, fieldNames)
                        : insertRecordToKudu(kuduTable, record, fieldNames);

                    // We keep track of mappings between Operations and their origins,
                    // so that we know which FlowFiles should be marked failure after buffered flush.
                    operationFlowFileMap.put(operation, flowFile);

                    // Flush mutation buffer of KuduSession to avoid "MANUAL_FLUSH is enabled
                    // but the buffer is too big" error. This can happen when flush mode is
                    // MANUAL_FLUSH and a FlowFile has more than one records.
                    if (numBuffered == batchSize && flushMode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
                        numBuffered = 0;
                        flushKuduSession(kuduSession, false, pendingRowErrors);
                    }

                    // OperationResponse is returned only when flush mode is set to AUTO_FLUSH_SYNC
                    OperationResponse response = kuduSession.apply(operation);
                    if (response != null && response.hasRowError()) {
                        // Stop processing the records on the first error.
                        // Note that Kudu does not support rolling back of previous operations.
                        flowFileFailures.put(flowFile, response.getRowError());
                        break;
                    }

                    numBuffered++;
                    numRecords.merge(flowFile, 1, Integer::sum);
                    record = recordSet.next();
                }
            } catch (Exception ex) {
                flowFileFailures.put(flowFile, ex);
            }
        }

        if (numBuffered > 0) {
            try {
                flushKuduSession(kuduSession, true, pendingRowErrors);
            } catch (final Exception e) {
                getLogger().error("Failed to flush/close Kudu Session", e);
                for (final FlowFile flowFile : flowFiles) {
                    session.transfer(flowFile, REL_FAILURE);
                }

                return;
            }
        }

        // Find RowErrors for each FlowFile
        final Map<FlowFile, List<RowError>> flowFileRowErrors = pendingRowErrors.stream().collect(
            Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation())));

        long totalCount = 0L;
        for (final FlowFile flowFile : flowFiles) {
            final int count = numRecords.getOrDefault(flowFile, 0);
            totalCount += count;
            final List<RowError> rowErrors = flowFileRowErrors.get(flowFile);

            if (rowErrors != null) {
                rowErrors.forEach(rowError -> getLogger().error("Failed to write due to {}", new Object[]{rowError.toString()}));
                session.putAttribute(flowFile, RECORD_COUNT_ATTR, String.valueOf(count - rowErrors.size()));
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.putAttribute(flowFile, RECORD_COUNT_ATTR, String.valueOf(count));

                if (flowFileFailures.containsKey(flowFile)) {
                    getLogger().error("Failed to write due to {}", new Object[]{flowFileFailures.get(flowFile)});
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().send(flowFile, "Successfully added FlowFile to Kudu");
                }
            }
        }

        session.adjustCounter("Records Inserted", totalCount, false);
    }


    protected KuduSession getKuduSession(final KuduClient client) {
        final KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(batchSize);
        kuduSession.setFlushMode(flushMode);

        if (operationType == OperationType.INSERT_IGNORE) {
            kuduSession.setIgnoreAllDuplicateRows(true);
        }

        return kuduSession;
    }

    private void flushKuduSession(final KuduSession kuduSession, boolean close, final List<RowError> rowErrors) throws KuduException {
        final List<OperationResponse> responses = close ? kuduSession.close() : kuduSession.flush();

        if (kuduSession.getFlushMode() == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
            rowErrors.addAll(Arrays.asList(kuduSession.getPendingErrors().getRowErrors()));
        } else {
            responses.stream()
                .filter(OperationResponse::hasRowError)
                .map(OperationResponse::getRowError)
                .forEach(rowErrors::add);
        }
    }



    protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) throws IllegalStateException, Exception {
        Upsert upsert = kuduTable.newUpsert();
        this.buildPartialRow(kuduTable.getSchema(), upsert.getRow(), record, fieldNames);
        return upsert;
    }

    protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) throws IllegalStateException, Exception {
        Insert insert = kuduTable.newInsert();
        this.buildPartialRow(kuduTable.getSchema(), insert.getRow(), record, fieldNames);
        return insert;
    }

    @VisibleForTesting
    void buildPartialRow(Schema schema, PartialRow row, Record record, List<String> fieldNames) {
        for (String colName : fieldNames) {
            int colIdx = this.getColumnIndex(schema, colName);
            if (colIdx != -1) {
                ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
                Type colType = colSchema.getType();

                if (record.getValue(colName) == null) {
                    row.setNull(colName);
                    continue;
                }

                switch (colType.getDataType(colSchema.getTypeAttributes())) {
                    case BOOL:
                        row.addBoolean(colIdx, record.getAsBoolean(colName));
                        break;
                    case FLOAT:
                        row.addFloat(colIdx, record.getAsFloat(colName));
                        break;
                    case DOUBLE:
                        row.addDouble(colIdx, record.getAsDouble(colName));
                        break;
                    case BINARY:
                        row.addBinary(colIdx, record.getAsString(colName).getBytes());
                        break;
                    case INT8:
                        row.addByte(colIdx, record.getAsInt(colName).byteValue());
                        break;
                    case INT16:
                        row.addShort(colIdx, record.getAsInt(colName).shortValue());
                        break;
                    case INT32:
                        row.addInt(colIdx, record.getAsInt(colName));
                        break;
                    case INT64:
                    case UNIXTIME_MICROS:
                        row.addLong(colIdx, record.getAsLong(colName));
                        break;
                    case STRING:
                        row.addString(colIdx, record.getAsString(colName));
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        row.addDecimal(colIdx, new BigDecimal(record.getAsString(colName)));
                        break;
                    default:
                        throw new IllegalStateException(String.format("unknown column type %s", colType));
                }
            }
        }
    }

    private int getColumnIndex(Schema columns, String colName) {
        try {
            return columns.getColumnIndex(colName);
        } catch (Exception ex) {
            return -1;
        }
    }
}
