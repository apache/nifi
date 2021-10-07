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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchError;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
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
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleDateFormatValidator;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"json", "elasticsearch", "elasticsearch5", "elasticsearch6", "put", "index", "record"})
@CapabilityDescription("A record-aware Elasticsearch put processor that uses the official Elastic REST client libraries.")
@DynamicProperty(
        name = "The name of a URL query parameter to add",
        value = "The value of the URL query parameter",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                "These parameters will override any matching parameters in the query request body")
public class PutElasticsearchRecord extends AbstractProcessor implements ElasticsearchRestProcessor {
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("put-es-record-reader")
        .displayName("Record Reader")
        .description("The record reader to use for reading incoming records from flowfiles.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("put-es-record-batch-size")
        .displayName("Batch Size")
        .description("The number of records to send over in a single batch.")
        .defaultValue("100")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    static final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
        .name("put-es-record-index-op")
        .displayName("Index Operation")
        .description("The type of the operation used to index (create, delete, index, update, upsert)")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(IndexOperationRequest.Operation.Index.getValue())
        .required(true)
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

    static final PropertyDescriptor AT_TIMESTAMP_DATE_FORMAT = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp-date-format")
        .displayName("@Timestamp Record Path Date Format")
        .description("Specifies the format to use when writing Date field for @timestamp. "
                + "If not specified, the default format '" + RecordFieldType.DATE.getDefaultFormat() + "' is used. "
                + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy for a two-digit month, followed by "
                + "a two-digit day, followed by a four-digit year, all separated by '/' characters, as in 01/25/2017).")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .dependsOn(AT_TIMESTAMP_RECORD_PATH)
        .build();

    static final PropertyDescriptor AT_TIMESTAMP_TIME_FORMAT = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp-time-format")
        .displayName("@Timestamp Record Path Time Format")
        .description("Specifies the format to use when writing Time field for @timestamp. "
                + "If not specified, the default format '" + RecordFieldType.TIME.getDefaultFormat() + "' is used. "
                + "If specified, the value must match the Java Simple Date Format (for example, HH:mm:ss for a two-digit hour in 24-hour format, followed by "
                + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 18:04:15).")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .dependsOn(AT_TIMESTAMP_RECORD_PATH)
        .build();

    static final PropertyDescriptor AT_TIMESTAMP_TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
        .name("put-es-record-at-timestamp-timestamp-format")
        .displayName("@Timestamp Record Path Timestamp Format")
        .description("Specifies the format to use when writing Timestamp field for @timestamp. "
                + "If not specified, the default format '" + RecordFieldType.TIMESTAMP.getDefaultFormat() + "' is used. "
                + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy HH:mm:ss for a two-digit month, followed by "
                + "a two-digit day, followed by a four-digit year, all separated by '/' characters; and then followed by a two-digit hour in 24-hour format, followed by "
                + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 01/25/2017 18:04:15).")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(new SimpleDateFormatValidator())
        .required(false)
        .dependsOn(AT_TIMESTAMP_RECORD_PATH)
        .build();

    static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        INDEX_OP, INDEX, TYPE, AT_TIMESTAMP, CLIENT_SERVICE, RECORD_READER, BATCH_SIZE, ID_RECORD_PATH, RETAIN_ID_FIELD,
        INDEX_OP_RECORD_PATH, INDEX_RECORD_PATH, TYPE_RECORD_PATH, AT_TIMESTAMP_RECORD_PATH, RETAIN_AT_TIMESTAMP_FIELD,
        AT_TIMESTAMP_DATE_FORMAT, AT_TIMESTAMP_TIME_FORMAT, AT_TIMESTAMP_TIMESTAMP_FORMAT, LOG_ERROR_RESPONSES,
        ERROR_RECORD_WRITER
    ));
    static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_RETRY, REL_FAILED_RECORDS
    )));

    static final List<String> ALLOWED_INDEX_OPERATIONS = Collections.unmodifiableList(Arrays.asList(
            IndexOperationRequest.Operation.Create.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Delete.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Index.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Update.getValue().toLowerCase(),
            IndexOperationRequest.Operation.Upsert.getValue().toLowerCase()
    ));

    private RecordPathCache recordPathCache;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private boolean logErrors;
    private ObjectMapper errorMapper;

    private volatile ElasticSearchClientService clientService;
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

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        this.clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
        this.recordPathCache = new RecordPathCache(16);
        this.writerFactory = context.getProperty(ERROR_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        this.logErrors = context.getProperty(LOG_ERROR_RESPONSES).asBoolean();

        this.dateFormat = context.getProperty(AT_TIMESTAMP_DATE_FORMAT).evaluateAttributeExpressions().getValue();
        if (this.dateFormat == null) {
            this.dateFormat = RecordFieldType.DATE.getDefaultFormat();
        }
        this.timeFormat = context.getProperty(AT_TIMESTAMP_TIME_FORMAT).evaluateAttributeExpressions().getValue();
        if (this.timeFormat == null) {
            this.timeFormat = RecordFieldType.TIME.getDefaultFormat();
        }
        this.timestampFormat = context.getProperty(AT_TIMESTAMP_TIMESTAMP_FORMAT).evaluateAttributeExpressions().getValue();
        if (this.timestampFormat == null) {
            this.timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();
        }

        if (errorMapper == null && (logErrors || getLogger().isDebugEnabled())) {
            errorMapper = new ObjectMapper();
            errorMapper.enable(SerializationFeature.INDENT_OUTPUT);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();

        final PropertyValue indexOp = validationContext.getProperty(INDEX_OP);
        final ValidationResult.Builder indexOpValidationResult = new ValidationResult.Builder().subject(INDEX_OP.getName());
        if (!indexOp.isExpressionLanguagePresent()) {
            final String indexOpValue = indexOp.evaluateAttributeExpressions().getValue();
            indexOpValidationResult.input(indexOpValue);
            if (!ALLOWED_INDEX_OPERATIONS.contains(indexOpValue.toLowerCase())) {
                indexOpValidationResult.valid(false)
                        .explanation(String.format("%s must be Expression Language or one of %s",
                                INDEX_OP.getDisplayName(), ALLOWED_INDEX_OPERATIONS)
                        );
            } else {
                indexOpValidationResult.valid(true);
            }
        } else {
            indexOpValidationResult.valid(true).input(indexOp.getValue()).explanation("Expression Language present");
        }
        validationResults.add(indexOpValidationResult.build());

        return validationResults;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile input = session.get();
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
                final String t   = getFromRecordPath(record, tPath, type, false);
                final IndexOperationRequest.Operation o = IndexOperationRequest.Operation.forValue(getFromRecordPath(record, ioPath, indexOp, false));
                final String id  = getFromRecordPath(record, path, null, retainId);
                final Object timestamp = getTimestampFromRecordPath(record, atPath, atTimestamp, retainTimestamp);

                @SuppressWarnings("unchecked")
                final Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils
                        .convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
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
        } catch (final ElasticsearchError ese) {
            final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                    ese.isElastic() ? "Moving to retry." : "Moving to failure");
            getLogger().error(msg, ese);
            final Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
            session.penalize(input);
            session.transfer(input, rel);
            removeBadRecordFlowFiles(badRecords, session);
            return;
        } catch (final Exception ex) {
            getLogger().error("Could not index documents.", ex);
            session.transfer(input, REL_FAILURE);
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

    private FlowFile indexDocuments(final BulkOperation bundle, final ProcessContext context, final ProcessSession session, final FlowFile input) throws Exception {
        final IndexOperationResponse response = clientService.bulk(bundle.getOperationList(), getUrlQueryParameters(context, input));
        if (response.hasErrors()) {
            if (logErrors || getLogger().isDebugEnabled()) {
                final List<Map<String, Object>> errors = response.getItems();
                final String output = String.format("An error was encountered while processing bulk operations. Server response below:%n%n%s", errorMapper.writeValueAsString(errors));

                if (logErrors) {
                    getLogger().error(output);
                } else {
                    getLogger().debug(output);
                }
            }

            if (writerFactory != null) {
                FlowFile errorFF = session.create(input);
                try {
                    int added = 0;
                    try (final OutputStream os = session.write(errorFF);
                         final RecordSetWriter writer = writerFactory.createWriter(getLogger(), bundle.getSchema(), os, errorFF )) {

                        writer.beginRecordSet();
                        for (int index = 0; index < response.getItems().size(); index++) {
                            final Map<String, Object> current = response.getItems().get(index);
                            if (!current.isEmpty()) {
                                final String key = current.keySet().stream().findFirst().orElse(null);
                                @SuppressWarnings("unchecked")
                                final Map<String, Object> inner = (Map<String, Object>) current.get(key);
                                if (inner != null && inner.containsKey("error")) {
                                    writer.write(bundle.getOriginalRecords().get(index));
                                    added++;
                                }
                            }
                        }
                        writer.finishRecordSet();
                    }

                    errorFF = session.putAttribute(errorFF, ATTR_RECORD_COUNT, String.valueOf(added));

                    session.transfer(errorFF, REL_FAILED_RECORDS);

                    return errorFF;
                } catch (final Exception ex) {
                    getLogger().error("", ex);
                    session.remove(errorFF);
                    throw ex;
                }
            }
        }
        return null;
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
            default:
                format = this.timestampFormat;
        }
        return format;
    }

    private Object coerceStringToLong(final String fieldName, final String stringValue) {
        return DataTypeUtils.isLongTypeCompatible(stringValue)
                ? DataTypeUtils.toLong(stringValue, fieldName)
                : stringValue;
    }
}
