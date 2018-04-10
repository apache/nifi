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

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.client.SessionConfiguration;

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
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractKudu extends AbstractProcessor {

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

    protected String kuduMasters;
    protected String tableName;
    protected boolean skipHeadLine;
    protected OperationType operationType;
    protected SessionConfiguration.FlushMode flushMode;
    protected int batchSize = 100;

    protected KuduClient kuduClient;
    protected KuduTable kuduTable;

    @OnScheduled
    public void OnScheduled(final ProcessContext context) {
        try {
            tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
            kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
            operationType = OperationType.valueOf(context.getProperty(INSERT_OPERATION).getValue());
            batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
            flushMode = SessionConfiguration.FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue());
            skipHeadLine = context.getProperty(SKIP_HEAD_LINE).asBoolean();

            if (kuduClient == null) {
                getLogger().debug("Setting up Kudu connection...");
                kuduClient = getKuduConnection(kuduMasters);
                kuduTable = this.getKuduTable(kuduClient, tableName);
                getLogger().debug("Kudu connection successfully initialized");
            }
        } catch(KuduException ex){
            getLogger().error("Exception occurred while interacting with Kudu due to " + ex.getMessage(), ex);
        }
    }

    @OnStopped
    public final void closeClient() throws KuduException {
        if (kuduClient != null) {
            getLogger().info("Closing KuduClient");
            kuduClient.close();
            kuduClient = null;
        }
    }

    private Stream<RowError> flushKuduSession(final KuduSession kuduSession, boolean close) throws Exception {
        List<OperationResponse> responses = close ? kuduSession.close() : kuduSession.flush();
        Stream<RowError> rowErrors;
        if (kuduSession.getFlushMode() == FlushMode.AUTO_FLUSH_BACKGROUND) {
            rowErrors = Stream.of(kuduSession.getPendingErrors().getRowErrors());
        } else {
            rowErrors = responses.stream()
                .filter(OperationResponse::hasRowError)
                .map(OperationResponse::getRowError);
        }
        return rowErrors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) return;

        final KuduSession kuduSession = getKuduSession(kuduClient);
        final RecordReaderFactory recordReaderFactory =
            context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final Map<FlowFile, Integer> numRecords = new HashMap<>();
        final Map<FlowFile, Object> flowFileFailures = new HashMap<>();
        final Map<Operation, FlowFile> operationFlowFileMap = new HashMap<>();

        int numBuffered = 0;
        Stream<RowError> pendingRowErrors = Stream.empty();
        for (FlowFile flowFile : flowFiles) {
            try (final InputStream in = session.read(flowFile);
                final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger())) {
                final List<String> fieldNames = recordReader.getSchema().getFieldNames();
                final RecordSet recordSet = recordReader.createRecordSet();

                // Deprecated
                if (skipHeadLine) recordSet.next();

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
                    if (numBuffered == batchSize && flushMode == FlushMode.MANUAL_FLUSH) {
                        numBuffered = 0;
                        pendingRowErrors = Stream.concat(pendingRowErrors, flushKuduSession(kuduSession, false));
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

        try {
            // Find RowErrors for each FlowFile
            Map<FlowFile, List<RowError>> flowFileRowErrors =
                Stream.concat(
                    pendingRowErrors,
                    numBuffered > 0 ? flushKuduSession(kuduSession, true) : Stream.empty()
                ).collect(Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation())));

            flowFiles.forEach(ff -> {
                int count = numRecords.getOrDefault(ff, 0);
                List<RowError> rowErrors = flowFileRowErrors.get(ff);
                if (rowErrors != null) {
                    rowErrors.forEach(rowError ->
                        getLogger().error("Failed to write due to {}", new Object[]{rowError}));
                    session.putAttribute(ff, RECORD_COUNT_ATTR, String.valueOf(count - rowErrors.size()));
                    session.transfer(ff, REL_FAILURE);
                } else {
                    session.putAttribute(ff, RECORD_COUNT_ATTR, String.valueOf(count));
                    if (flowFileFailures.containsKey(ff)) {
                        getLogger().error("Failed to write due to {}", new Object[]{flowFileFailures.get(ff)});
                        session.transfer(ff, REL_FAILURE);
                    } else {
                        session.transfer(ff, REL_SUCCESS);
                        session.getProvenanceReporter().send(ff, "Successfully added FlowFile to Kudu");
                    }
                }
            });
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }

    protected KuduClient getKuduConnection(String masters) {
        return new KuduClient.KuduClientBuilder(kuduMasters).build();
    }

    protected KuduTable getKuduTable(KuduClient client, String tableName) throws KuduException {
        return client.openTable(tableName);
    }

    protected KuduSession getKuduSession(KuduClient client){

        KuduSession kuduSession = client.newSession();

        kuduSession.setMutationBufferSpace(batchSize);
        kuduSession.setFlushMode(flushMode);

        if(operationType == OperationType.INSERT_IGNORE){
            kuduSession.setIgnoreAllDuplicateRows(true);
        }

        return kuduSession;
    }

    protected abstract Insert insertRecordToKudu(final KuduTable table, final Record record, final List<String> fields) throws Exception;
    protected abstract Upsert upsertRecordToKudu(final KuduTable table, final Record record, final List<String> fields) throws Exception;
}

