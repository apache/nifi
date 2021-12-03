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

package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.elasticsearch.api.BulkOperation;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleDateFormatValidator;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"json", "elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "put", "index", "record"})
@CapabilityDescription("A record-aware Elasticsearch put processor that uses the official Elastic REST client libraries.")
@WritesAttributes({
        @WritesAttribute(attribute = "elasticsearch.put.error", description = "The error message provided by Elasticsearch if there is an error indexing the documents.")
})
@DynamicProperty(
        name = "The name of a URL query parameter to add",
        value = "The value of the URL query parameter",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                "These parameters will override any matching parameters in the _bulk request body")
@SystemResourceConsideration(
        resource = SystemResource.MEMORY,
        description = "The Batch of Records will be stored in memory until the bulk operation is performed.")
public class PutElasticsearchRecord extends AbstractPutElasticsearch {
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("put-es-record-reader")
        .displayName("Record Reader")
        .description("The record reader to use for reading incoming records from flowfiles.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(AbstractPutElasticsearch.BATCH_SIZE)
        .description("The number of records to send over in a single batch.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor AT_TIMESTAMP = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp")
        .displayName("@timestamp Value")
        .description("The value to use as the @timestamp field (required for Elasticsearch Data Streams)")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();

    static final PropertyDescriptor INDEX_OP_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("put-es-record-index-op-path")
        .displayName("Index Operation Record Path")
        .description("A record path expression to retrieve the Index Operation field for use with Elasticsearch. If left blank " +
                "the Index Operation will be determined using the main Index Operation property.")
        .addValidator(new RecordPathValidator())
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor ID_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("put-es-record-id-path")
        .displayName("ID Record Path")
        .description("A record path expression to retrieve the ID field for use with Elasticsearch. If left blank " +
                "the ID will be automatically generated by Elasticsearch.")
        .addValidator(new RecordPathValidator())
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor RETAIN_ID_FIELD = new PropertyDescriptor.Builder()
        .name("put-es-record-retain-id-field")
        .displayName("Retain ID (Record Path)")
        .description("Whether to retain the existing field used as the ID Record Path.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(false)
        .dependsOn(ID_RECORD_PATH)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor INDEX_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("put-es-record-index-record-path")
        .displayName("Index Record Path")
        .description("A record path expression to retrieve the index field for use with Elasticsearch. If left blank " +
                "the index will be determined using the main index property.")
        .addValidator(new RecordPathValidator())
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor TYPE_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("put-es-record-type-record-path")
        .displayName("Type Record Path")
        .description("A record path expression to retrieve the type field for use with Elasticsearch. If left blank " +
                "the type will be determined using the main type property.")
        .addValidator(new RecordPathValidator())
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor AT_TIMESTAMP_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp-path")
        .displayName("@timestamp Record Path")
        .description("A RecordPath pointing to a field in the record(s) that contains the @timestamp for the document. " +
                "If left blank the @timestamp will be determined using the main @timestamp property")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor RETAIN_AT_TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
        .name("put-es-record-retain-at-timestamp-field")
        .displayName("Retain @timestamp (Record Path)")
        .description("Whether to retain the existing field used as the @timestamp Record Path.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(false)
        .dependsOn(AT_TIMESTAMP_RECORD_PATH)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor ERROR_RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("put-es-record-error-writer")
        .displayName("Error Record Writer")
        .description("If this configuration property is set, the response from Elasticsearch will be examined for failed records " +
                "and the failed records will be written to a record set with this record writer service and sent to the \"errors\" " +
                "relationship.")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp-date-format")
        .displayName("Date Format")
        .description("Specifies the format to use when writing Date fields. "
                + "If not specified, the default format '" + RecordFieldType.DATE.getDefaultFormat() + "' is used. "
                + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy for a two-digit month, followed by "
                + "a two-digit day, followed by a four-digit year, all separated by '/' characters, as in 01/25/2017).")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .build();

    static final PropertyDescriptor TIME_FORMAT = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp-time-format")
        .displayName("Time Format")
        .description("Specifies the format to use when writing Time fields. "
                + "If not specified, the default format '" + RecordFieldType.TIME.getDefaultFormat() + "' is used. "
                + "If specified, the value must match the Java Simple Date Format (for example, HH:mm:ss for a two-digit hour in 24-hour format, followed by "
                + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 18:04:15).")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .build();

    static final PropertyDescriptor TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp-timestamp-format")
        .displayName("Timestamp Format")
        .description("Specifies the format to use when writing Timestamp fields. "
                + "If not specified, the default format '" + RecordFieldType.TIMESTAMP.getDefaultFormat() + "' is used. "
                + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy HH:mm:ss for a two-digit month, followed by "
                + "a two-digit day, followed by a four-digit year, all separated by '/' characters; and then followed by a two-digit hour in 24-hour format, followed by "
                + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 01/25/2017 18:04:15).")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .build();

    static final Relationship REL_FAILED_RECORDS = new Relationship.Builder()
            .name("errors").description("If an Error Record Writer is set, any record that failed to process the way it was " +
                    "configured will be sent to this relationship as part of a failed record set.")
            .autoTerminateDefault(true).build();

    static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        INDEX_OP, INDEX, TYPE, AT_TIMESTAMP, CLIENT_SERVICE, RECORD_READER, BATCH_SIZE, ID_RECORD_PATH, RETAIN_ID_FIELD,
        INDEX_OP_RECORD_PATH, INDEX_RECORD_PATH, TYPE_RECORD_PATH, AT_TIMESTAMP_RECORD_PATH, RETAIN_AT_TIMESTAMP_FIELD,
        DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, LOG_ERROR_RESPONSES, ERROR_RECORD_WRITER
    ));
    static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_RETRY, REL_FAILED_RECORDS
    )));

    private RecordPathCache recordPathCache;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;

    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);

        this.readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        this.recordPathCache = new RecordPathCache(16);
        this.writerFactory = context.getProperty(ERROR_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        this.dateFormat = context.getProperty(DATE_FORMAT).evaluateAttributeExpressions().getValue();
        if (this.dateFormat == null) {
            this.dateFormat = RecordFieldType.DATE.getDefaultFormat();
        }
        this.timeFormat = context.getProperty(TIME_FORMAT).evaluateAttributeExpressions().getValue();
        if (this.timeFormat == null) {
            this.timeFormat = RecordFieldType.TIME.getDefaultFormat();
        }
        this.timestampFormat = context.getProperty(TIMESTAMP_FORMAT).evaluateAttributeExpressions().getValue();
        if (this.timestampFormat == null) {
            this.timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(input).getValue();
        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
        final String type  = context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue();
        final String atTimestamp  = context.getProperty(AT_TIMESTAMP).evaluateAttributeExpressions(input).getValue();

        final String indexOpPath = context.getProperty(INDEX_OP_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
        final String idPath = context.getProperty(ID_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
        final String indexPath = context.getProperty(INDEX_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
        final String typePath = context.getProperty(TYPE_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
        final String atTimestampPath = context.getProperty(AT_TIMESTAMP_RECORD_PATH).evaluateAttributeExpressions(input).getValue();

        final RecordPath ioPath = indexOpPath != null ? recordPathCache.getCompiled(indexOpPath) : null;
        final RecordPath path = idPath != null ? recordPathCache.getCompiled(idPath) : null;
        final RecordPath iPath = indexPath != null ? recordPathCache.getCompiled(indexPath) : null;
        final RecordPath tPath = typePath != null ? recordPathCache.getCompiled(typePath) : null;
        final RecordPath atPath = atTimestampPath != null ? recordPathCache.getCompiled(atTimestampPath) : null;

        final boolean retainId = context.getProperty(RETAIN_ID_FIELD).evaluateAttributeExpressions(input).asBoolean();
        final boolean retainTimestamp = context.getProperty(RETAIN_AT_TIMESTAMP_FIELD).evaluateAttributeExpressions(input).asBoolean();

        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(input).asInteger();
        final List<FlowFile> badRecords = new ArrayList<>();

        try (final InputStream inStream = session.read(input);
            final RecordReader reader = readerFactory.createRecordReader(input, inStream, getLogger())) {
            final PushBackRecordSet recordSet = new PushBackRecordSet(reader.createRecordSet());
            final List<IndexOperationRequest> operationList = new ArrayList<>();
            final List<Record> originals = new ArrayList<>();

            Record record;
            while ((record = recordSet.next()) != null) {
                final String idx = getFromRecordPath(record, iPath, index, false);
                final String t = getFromRecordPath(record, tPath, type, false);
                final IndexOperationRequest.Operation o = IndexOperationRequest.Operation.forValue(getFromRecordPath(record, ioPath, indexOp, false));
                final String id = getFromRecordPath(record, path, null, retainId);
                final Object timestamp = getTimestampFromRecordPath(record, atPath, atTimestamp, retainTimestamp);

                @SuppressWarnings("unchecked")
                final Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils
                        .convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
                formatDateTimeFields(contentMap, record);
                contentMap.putIfAbsent("@timestamp", timestamp);

                operationList.add(new IndexOperationRequest(idx, t, id, contentMap, o));
                originals.add(record);

                if (operationList.size() == batchSize || !recordSet.isAnotherRecord()) {
                    final BulkOperation bundle = new BulkOperation(operationList, originals, reader.getSchema());
                    final FlowFile bad = indexDocuments(bundle, context, session, input);
                    if (bad != null) {
                        badRecords.add(bad);
                    }

                    operationList.clear();
                    originals.clear();
                }
            }

            if (!operationList.isEmpty()) {
                final BulkOperation bundle = new BulkOperation(operationList, originals, reader.getSchema());
                final FlowFile bad = indexDocuments(bundle, context, session, input);
                if (bad != null) {
                    badRecords.add(bad);
                }
            }
        } catch (final ElasticsearchException ese) {
            final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                    ese.isElastic() ? "Routing to retry." : "Routing to failure");
            getLogger().error(msg, ese);
            final Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
            transferFlowFilesOnException(ese, rel, session, true, input);
            removeBadRecordFlowFiles(badRecords, session);
            return;
        } catch (final IOException | SchemaNotFoundException ex) {
            getLogger().warn("Could not log Elasticsearch operation errors nor determine which documents errored.", ex);
            final Relationship rel = writerFactory != null ? REL_FAILED_RECORDS : REL_FAILURE;
            transferFlowFilesOnException(ex, rel, session, true, input);
            removeBadRecordFlowFiles(badRecords, session);
            return;
        } catch (final Exception ex) {
            getLogger().error("Could not index documents.", ex);
            transferFlowFilesOnException(ex, REL_FAILURE, session, false, input);
            context.yield();
            removeBadRecordFlowFiles(badRecords, session);
            return;
        }
        session.transfer(input, REL_SUCCESS);
    }

    private void removeBadRecordFlowFiles(final List<FlowFile> bad, final ProcessSession session) {
        for (final FlowFile badFlowFile : bad) {
            session.remove(badFlowFile);
        }

        bad.clear();
    }

    private FlowFile indexDocuments(final BulkOperation bundle, final ProcessContext context, final ProcessSession session, final FlowFile input) throws IOException, SchemaNotFoundException {
        final IndexOperationResponse response = clientService.bulk(bundle.getOperationList(), getUrlQueryParameters(context, input));
        if (response.hasErrors()) {
            logElasticsearchDocumentErrors(response);

            if (writerFactory != null) {
                FlowFile errorFF = session.create(input);
                try {
                    int added = 0;
                    try (final OutputStream os = session.write(errorFF);
                         final RecordSetWriter writer = writerFactory.createWriter(getLogger(), bundle.getSchema(), os, errorFF )) {

                        writer.beginRecordSet();
                        for (final int index : findElasticsearchErrorIndices(response)) {
                            writer.write(bundle.getOriginalRecords().get(index));
                            added++;
                        }
                        writer.finishRecordSet();
                    }

                    errorFF = session.putAttribute(errorFF, ATTR_RECORD_COUNT, String.valueOf(added));

                    session.transfer(errorFF, REL_FAILED_RECORDS);

                    return errorFF;
                } catch (final IOException | SchemaNotFoundException ex) {
                    getLogger().error("Unable to write error records", ex);
                    session.remove(errorFF);
                    throw ex;
                }
            }
        }
        return null;
    }

    private void formatDateTimeFields(final Map<String, Object> contentMap, final Record record) {
        for (final RecordField recordField : record.getSchema().getFields()) {
            final Object value = contentMap.get(recordField.getFieldName());
            if (value != null) {
                final DataType chosenDataType = recordField.getDataType().getFieldType() == RecordFieldType.CHOICE
                        ? DataTypeUtils.chooseDataType(record.getValue(recordField), (ChoiceDataType) recordField.getDataType())
                        : recordField.getDataType();

                final String format = determineDateFormat(chosenDataType.getFieldType());
                if (format != null) {
                    final Object formattedValue = coerceStringToLong(
                            recordField.getFieldName(),
                            DataTypeUtils.toString(value, () -> DataTypeUtils.getDateFormat(format))
                    );
                    contentMap.put(recordField.getFieldName(), formattedValue);
                }
            }
        }
    }

    private String getFromRecordPath(final Record record, final RecordPath path, final String fallback,
                                     final boolean retain) {
        if (path == null) {
            return fallback;
        }

        final RecordPathResult result = path.evaluate(record);
        final Optional<FieldValue> value = result.getSelectedFields().findFirst();
        if (value.isPresent() && value.get().getValue() != null) {
            final FieldValue fieldValue = value.get();
            if (!fieldValue.getField().getDataType().getFieldType().equals(RecordFieldType.STRING) ) {
                throw new ProcessException(
                    String.format("Field referenced by %s must be a string.", path.getPath())
                );
            }

            if (!retain) {
                fieldValue.updateValue(null);
            }

            return fieldValue.getValue().toString();
        } else {
            return fallback;
        }
    }

    private Object getTimestampFromRecordPath(final Record record, final RecordPath path, final String fallback,
                                              final boolean retain) {
        if (path == null) {
            return coerceStringToLong("@timestamp", fallback);
        }

        final RecordPathResult result = path.evaluate(record);
        final Optional<FieldValue> value = result.getSelectedFields().findFirst();
        if (value.isPresent() && value.get().getValue() != null) {
            final FieldValue fieldValue = value.get();

            final DataType dataType = fieldValue.getField().getDataType();
            final String fieldName = fieldValue.getField().getFieldName();
            final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE
                    ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType)
                    : dataType;
            final Object coercedValue = DataTypeUtils.convertType(fieldValue.getValue(), chosenDataType, fieldName);
            if (coercedValue == null) {
                return null;
            }

            final Object returnValue;
            switch (chosenDataType.getFieldType()) {
                case DATE:
                case TIME:
                case TIMESTAMP:
                    final String format = determineDateFormat(chosenDataType.getFieldType());
                    returnValue = coerceStringToLong(
                            fieldName,
                            DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(format))
                    );
                    break;
                case LONG:
                    returnValue = DataTypeUtils.toLong(coercedValue, fieldName);
                    break;
                case INT:
                case BYTE:
                case SHORT:
                    returnValue = DataTypeUtils.toInteger(coercedValue, fieldName);
                    break;
                case CHAR:
                case STRING:
                    returnValue = coerceStringToLong(fieldName, coercedValue.toString());
                    break;
                case BIGINT:
                    returnValue = coercedValue;
                    break;
                default:
                    throw new ProcessException(
                            String.format("Cannot use %s field referenced by %s as @timestamp.", chosenDataType, path.getPath())
                    );
            }

            if (!retain) {
                fieldValue.updateValue(null);
            }

            return returnValue;
        } else {
            return coerceStringToLong("@timestamp", fallback);
        }
    }

    private String determineDateFormat(final RecordFieldType recordFieldType) {
        final String format;
        switch (recordFieldType) {
            case DATE:
                format = this.dateFormat;
                break;
            case TIME:
                format = this.timeFormat;
                break;
            case TIMESTAMP:
                format = this.timestampFormat;
                break;
            default:
                format = null;
        }
        return format;
    }

    private Object coerceStringToLong(final String fieldName, final String stringValue) {
        return DataTypeUtils.isLongTypeCompatible(stringValue)
                ? DataTypeUtils.toLong(stringValue, fieldName)
                : stringValue;
    }
}
