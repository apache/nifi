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

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSet;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EventDriven
@SupportsBatching
@RequiresInstanceClassLoading // Because of calls to UserGroupInformation.setConfiguration
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "database", "NoSQL", "kudu", "HDFS", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to the specified Kudu's table. The schema for the Kudu table is inferred from the schema of the Record Reader." +
        " If any error occurs while reading records from the input, or writing records to Kudu, the FlowFile will be routed to failure")
@WritesAttribute(attribute = "record.count", description = "Number of records written to Kudu")

public class PutKudu extends AbstractKuduProcessor {

    protected static final PropertyDescriptor TABLE_NAME = new Builder()
        .name("Table Name")
        .description("The name of the Kudu Table to put data into")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor RECORD_READER = new Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("The service for reading records from incoming flow files.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor SKIP_HEAD_LINE = new Builder()
        .name("Skip head line")
        .description("Deprecated. Used to ignore header lines, but this should be handled by a RecordReader " +
            "(e.g. \"Treat First Line as Header\" property of CSVReader)")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor LOWERCASE_FIELD_NAMES = new Builder()
            .name("Lowercase Field Names")
            .description("Convert column names to lowercase when finding index of Kudu table columns")
            .defaultValue("false")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor HANDLE_SCHEMA_DRIFT = new Builder()
            .name("Handle Schema Drift")
            .description("If set to true, when fields with names that are not in the target Kudu table " +
                    "are encountered, the Kudu table will be altered to include new columns for those fields.")
            .defaultValue("false")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final Validator OperationTypeValidator = new Validator() {
        @Override
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value)
                        .explanation("Expression Language Present").valid(true).build();
            }

            boolean valid;
            try {
                OperationType.valueOf(value.toUpperCase());
                valid = true;
            } catch (IllegalArgumentException ex) {
                valid = false;
            }

            final String explanation = valid ? null :
                    "Value must be one of: " +
                    Arrays.stream(OperationType.values()).map(Enum::toString).collect(Collectors.joining(", "));
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid)
                    .explanation(explanation).build();
        }
    };

    protected static final PropertyDescriptor INSERT_OPERATION = new Builder()
        .name("Insert Operation")
        .displayName("Kudu Operation Type")
        .description("Specify operationType for this processor.\n" +
                "Valid values are: " +
                Arrays.stream(OperationType.values()).map(Enum::toString).collect(Collectors.joining(", ")))
        .defaultValue(OperationType.INSERT.toString())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(OperationTypeValidator)
        .build();

    protected static final PropertyDescriptor FLUSH_MODE = new Builder()
        .name("Flush Mode")
        .description("Set the new flush mode for a kudu session.\n" +
            "AUTO_FLUSH_SYNC: the call returns when the operation is persisted, else it throws an exception.\n" +
            "AUTO_FLUSH_BACKGROUND: the call returns when the operation has been added to the buffer. This call should normally perform only fast in-memory" +
            " operations but it may have to wait when the buffer is full and there's another buffer being flushed.\n" +
            "MANUAL_FLUSH: the call returns when the operation has been added to the buffer, else it throws a KuduException if the buffer is full.")
        .allowableValues(SessionConfiguration.FlushMode.values())
        .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND.toString())
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();

    protected static final PropertyDescriptor FLOWFILE_BATCH_SIZE = new Builder()
        .name("FlowFiles per Batch")
        .description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. " +
            "Depending on your memory size, and data size per row set an appropriate batch size " +
            "for the number of FlowFiles to process per client connection setup." +
            "Gradually increase this number, only if your FlowFiles typically contain a few records.")
        .defaultValue("1")
        .required(true)
        .addValidator(StandardValidators.createLongValidator(1, 100000, true))
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    protected static final PropertyDescriptor BATCH_SIZE = new Builder()
        .name("Batch Size")
        .displayName("Max Records per Batch")
        .description("The maximum number of Records to process in a single Kudu-client batch, between 1 - 100000. " +
            "Depending on your memory size, and data size per row set an appropriate batch size. " +
            "Gradually increase this number to find out the best one for best performances.")
        .defaultValue("100")
        .required(true)
        .addValidator(StandardValidators.createLongValidator(1, 100000, true))
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();

    protected static final PropertyDescriptor IGNORE_NULL = new Builder()
        .name("Ignore NULL")
        .description("Ignore NULL on Kudu Put Operation, Update only non-Null columns if set true")
        .defaultValue("false")
        .required(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    // Properties set in onScheduled.
    protected int batchSize = 100;
    protected int ffbatch   = 1;
    protected SessionConfiguration.FlushMode flushMode;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(TABLE_NAME);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KERBEROS_PRINCIPAL);
        properties.add(KERBEROS_PASSWORD);
        properties.add(SKIP_HEAD_LINE);
        properties.add(LOWERCASE_FIELD_NAMES);
        properties.add(HANDLE_SCHEMA_DRIFT);
        properties.add(RECORD_READER);
        properties.add(INSERT_OPERATION);
        properties.add(FLUSH_MODE);
        properties.add(FLOWFILE_BATCH_SIZE);
        properties.add(BATCH_SIZE);
        properties.add(IGNORE_NULL);
        properties.add(KUDU_OPERATION_TIMEOUT_MS);
        properties.add(KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS);
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
    public void onScheduled(final ProcessContext context) throws IOException, LoginException {
        batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        ffbatch   = context.getProperty(FLOWFILE_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        flushMode = SessionConfiguration.FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue().toUpperCase());
        createKerberosUserAndOrKuduClient(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(ffbatch);
        if (flowFiles.isEmpty()) {
            return;
        }

        final KerberosUser user = getKerberosUser();
        if (user == null) {
            executeOnKuduClient(kuduClient -> trigger(context, session, flowFiles, kuduClient));
            return;
        }

        final PrivilegedExceptionAction<Void> privilegedAction = () -> {
            executeOnKuduClient(kuduClient -> trigger(context, session, flowFiles, kuduClient));
            return null;
        };

        final KerberosAction<Void> action = new KerberosAction<>(user, privilegedAction, getLogger());
        action.execute();
    }

    private void trigger(final ProcessContext context, final ProcessSession session, final List<FlowFile> flowFiles, KuduClient kuduClient) throws ProcessException {
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final KuduSession kuduSession = createKuduSession(kuduClient);

        final Map<FlowFile, Integer> numRecords = new HashMap<>();
        final Map<FlowFile, Object> flowFileFailures = new HashMap<>();
        final Map<Operation, FlowFile> operationFlowFileMap = new HashMap<>();

        int numBuffered = 0;
        OperationType prevOperationType = OperationType.INSERT;
        final List<RowError> pendingRowErrors = new ArrayList<>();
        for (FlowFile flowFile : flowFiles) {
            try (final InputStream in = session.read(flowFile);
                final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger())) {

                final String tableName = getEvaluatedProperty(TABLE_NAME, context, flowFile);
                final OperationType operationType = OperationType.valueOf(getEvaluatedProperty(INSERT_OPERATION, context, flowFile).toUpperCase());
                final Boolean ignoreNull = Boolean.valueOf(getEvaluatedProperty(IGNORE_NULL, context, flowFile));
                final Boolean lowercaseFields = Boolean.valueOf(getEvaluatedProperty(LOWERCASE_FIELD_NAMES, context, flowFile));
                final Boolean handleSchemaDrift = Boolean.valueOf(getEvaluatedProperty(HANDLE_SCHEMA_DRIFT, context, flowFile));

                final RecordSet recordSet = recordReader.createRecordSet();
                final List<String> fieldNames = recordReader.getSchema().getFieldNames();
                KuduTable kuduTable = kuduClient.openTable(tableName);

                // If handleSchemaDrift is true, check for any missing columns and alter the Kudu table to add them.
                if (handleSchemaDrift) {
                    final Schema schema = kuduTable.getSchema();
                    Stream<RecordField> fields = recordReader.getSchema().getFields().stream();
                    List<RecordField> missing = fields.filter(field -> !schema.hasColumn(
                            lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName()))
                            .collect(Collectors.toList());
                    if (!missing.isEmpty()) {
                        getLogger().info("adding {} columns to table '{}' to handle schema drift",
                                new Object[]{missing.size(), tableName});
                        // Add each column one at a time to avoid failing if some of the missing columns
                        // we created by a concurrent thread or application attempting to handle schema drift.
                        for (RecordField field : missing) {
                            try {
                                final String columnName = lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName();
                                kuduClient.alterTable(tableName, getAddNullableColumnStatement(columnName, field.getDataType()));
                            } catch (KuduException e) {
                                // Ignore the exception if the column already exists due to concurrent
                                // threads or applications attempting to handle schema drift.
                                if (e.getStatus().isAlreadyPresent()) {
                                    getLogger().info("column already exists in table '{}' while handling schema drift",
                                            new Object[]{tableName});
                                } else {
                                    throw new ProcessException(e);
                                }
                            }
                        }
                        // Re-open the table to get the new schema.
                        kuduTable = kuduClient.openTable(tableName);
                    }
                }

                // In the case of INSERT_IGNORE the Kudu session is modified to ignore row errors.
                // Because the session is shared across flow files, for batching efficiency, we
                // need to flush when changing to and from INSERT_IGNORE operation types.
                // This should be updated and simplified when KUDU-1563 is completed.
                if (prevOperationType != operationType && (prevOperationType == OperationType.INSERT_IGNORE || operationType == OperationType.INSERT_IGNORE)) {
                    flushKuduSession(kuduSession, false, pendingRowErrors);
                    kuduSession.setIgnoreAllDuplicateRows(operationType == OperationType.INSERT_IGNORE);
                }
                prevOperationType = operationType;

                Record record = recordSet.next();
                while (record != null) {
                    Operation operation = createKuduOperation(operationType, record, fieldNames, ignoreNull, lowercaseFields, kuduTable);
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

    private String getEvaluatedProperty(PropertyDescriptor property, ProcessContext context, FlowFile flowFile) {
        PropertyValue evaluatedProperty = context.getProperty(property).evaluateAttributeExpressions(flowFile);
        if (property.isRequired() && evaluatedProperty == null) {
            throw new ProcessException(String.format("Property `%s` is required but evaluated to null", property.getDisplayName()));
        }
        return evaluatedProperty.getValue();
    }

    protected KuduSession createKuduSession(final KuduClient client) {
        final KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(batchSize);
        kuduSession.setFlushMode(flushMode);
        return kuduSession;
    }

    private Operation createKuduOperation(OperationType operationType, Record record,
                                          List<String> fieldNames, Boolean ignoreNull,
                                          Boolean lowercaseFields, KuduTable kuduTable) {
        switch (operationType) {
            case DELETE:
                return deleteRecordFromKudu(kuduTable, record, fieldNames, ignoreNull, lowercaseFields);
            case INSERT:
            case INSERT_IGNORE:
                return insertRecordToKudu(kuduTable, record, fieldNames, ignoreNull, lowercaseFields);
            case UPSERT:
                return upsertRecordToKudu(kuduTable, record, fieldNames, ignoreNull, lowercaseFields);
            case UPDATE:
                return updateRecordToKudu(kuduTable, record, fieldNames, ignoreNull, lowercaseFields);
            default:
                throw new IllegalArgumentException(String.format("OperationType: %s not supported by Kudu", operationType));
        }
    }
}
