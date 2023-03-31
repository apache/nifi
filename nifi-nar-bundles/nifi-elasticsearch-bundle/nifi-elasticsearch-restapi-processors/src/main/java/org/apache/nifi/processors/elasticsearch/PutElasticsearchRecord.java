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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.nifi.annotation.behavior.DynamicProperties;
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
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
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
import org.apache.nifi.serialization.MalformedRecordException;
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
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"json", "elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "elasticsearch8", "put", "index", "record"})
@CapabilityDescription("A record-aware Elasticsearch put processor that uses the official Elastic REST client libraries.")
@WritesAttributes({
        @WritesAttribute(attribute = "elasticsearch.put.error",
                description = "The error message if there is an issue parsing the FlowFile records, sending the parsed documents to Elasticsearch or parsing the Elasticsearch response."),
        @WritesAttribute(attribute = "elasticsearch.put.error.count", description = "The number of records that generated errors in the Elasticsearch _bulk API."),
        @WritesAttribute(attribute = "elasticsearch.put.success.count", description = "The number of records that were successfully processed by the Elasticsearch _bulk API.")
})
@DynamicProperties({
        @DynamicProperty(
                name = "The name of the Bulk request header",
                value = "The value of the Bulk request header",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Prefix: " + AbstractPutElasticsearch.BULK_HEADER_PREFIX +
                        " - adds the specified property name/value as a Bulk request header in the Elasticsearch Bulk API body used for processing. " +
                        "These parameters will override any matching parameters in the _bulk request body."),
        @DynamicProperty(
                name = "The name of a URL query parameter to add",
                value = "The value of the URL query parameter",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                        "These parameters will override any matching parameters in the _bulk request body")
})
@SystemResourceConsideration(
        resource = SystemResource.MEMORY,
        description = "The Batch of Records will be stored in memory until the bulk operation is performed.")
public class PutElasticsearchRecord extends AbstractPutElasticsearch {
    static final Relationship REL_FAILED_RECORDS = new Relationship.Builder()
            .name("errors").description("If a \"Result Record Writer\" is set, any Record(s) corresponding to Elasticsearch document(s) " +
                    "that resulted in an \"error\" (within Elasticsearch) will be routed here.")
            .autoTerminateDefault(true).build();

    static final Relationship REL_SUCCESSFUL_RECORDS = new Relationship.Builder()
            .name("successful_records").description("If a \"Result Record Writer\" is set, any Record(s) corresponding to Elasticsearch document(s) " +
                    "that did not result in an \"error\" (within Elasticsearch) will be routed here.")
            .autoTerminateDefault(true).build();

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

    static final PropertyDescriptor SCRIPT_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("put-es-record-script-path")
            .displayName("Script Record Path")
            .description("A RecordPath pointing to a field in the record(s) that contains the script for the document update/upsert. " +
                    "Only applies to Update/Upsert operations. Field must be Map-type compatible (e.g. a Map or a Record) " +
                    "or a String parsable into a JSON Object")
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor SCRIPTED_UPSERT_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("put-es-record-scripted-upsert-path")
            .displayName("Scripted Upsert Record Path")
            .description("A RecordPath pointing to a field in the record(s) that contains the scripted_upsert boolean flag. " +
                    "Whether to add the scripted_upsert flag to the Upsert Operation. " +
                    "Forces Elasticsearch to execute the Script whether or not the document exists, defaults to false. " +
                    "If the Upsert Document provided (from FlowFile content) will be empty, but sure to set the " +
                    CLIENT_SERVICE.getDisplayName() + " controller service's " + ElasticSearchClientService.SUPPRESS_NULLS.getDisplayName() +
                    " to " + ElasticSearchClientService.NEVER_SUPPRESS.getDisplayName() + " or no \"upsert\" doc will be, " +
                    "included in the request to Elasticsearch and the operation will not create a new document for the script " +
                    "to execute against, resulting in a \"not_found\" error")
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DYNAMIC_TEMPLATES_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("put-es-record-dynamic-templates-path")
            .displayName("Dynamic Templates Record Path")
            .description("A RecordPath pointing to a field in the record(s) that contains the dynamic_templates for the document. " +
                    "Field must be Map-type compatible (e.g. a Map or Record) or a String parsable into a JSON Object. " +
                    "Requires Elasticsearch 7+")
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

    static final PropertyDescriptor RESULT_RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("put-es-record-error-writer")
        .displayName("Result Record Writer")
        .description("If this configuration property is set, the response from Elasticsearch will be examined for failed records " +
                "and the failed records will be written to a record set with this record writer service and sent to the \"" +
                REL_FAILED_RECORDS.getName() + "\" relationship. Successful records will be written to a record set " +
                "with this record writer service and sent to the \"" + REL_SUCCESSFUL_RECORDS.getName() + "\" relationship.")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    static final PropertyDescriptor NOT_FOUND_IS_SUCCESSFUL = new PropertyDescriptor.Builder()
        .name("put-es-record-not_found-is-error")
        .displayName("Treat \"Not Found\" as Success")
        .description("If true, \"not_found\" Elasticsearch Document associated Records will be routed to the \"" +
                REL_SUCCESSFUL_RECORDS.getName() + "\" relationship, otherwise to the \"" + REL_FAILED_RECORDS.getName() + "\" relationship. " +
                "If " + OUTPUT_ERROR_RESPONSES.getDisplayName() + " is \"true\" then \"not_found\" responses from Elasticsearch " +
                "will be sent to the " + REL_ERROR_RESPONSES.getName() + " relationship")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(false)
        .dependsOn(RESULT_RECORD_WRITER)
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

    static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        INDEX_OP, INDEX, TYPE, AT_TIMESTAMP, CLIENT_SERVICE, RECORD_READER, BATCH_SIZE, ID_RECORD_PATH, RETAIN_ID_FIELD,
        INDEX_OP_RECORD_PATH, INDEX_RECORD_PATH, TYPE_RECORD_PATH, AT_TIMESTAMP_RECORD_PATH, RETAIN_AT_TIMESTAMP_FIELD,
        SCRIPT_RECORD_PATH, SCRIPTED_UPSERT_RECORD_PATH, DYNAMIC_TEMPLATES_RECORD_PATH, DATE_FORMAT, TIME_FORMAT,
        TIMESTAMP_FORMAT, LOG_ERROR_RESPONSES, OUTPUT_ERROR_RESPONSES, RESULT_RECORD_WRITER, NOT_FOUND_IS_SUCCESSFUL
    ));
    static final Set<Relationship> BASE_RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_RETRY, REL_FAILED_RECORDS, REL_SUCCESSFUL_RECORDS
    )));

    private RecordPathCache recordPathCache;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;

    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @Override
    Set<Relationship> getBaseRelationships() {
        return BASE_RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);

        this.readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        this.recordPathCache = new RecordPathCache(16);
        this.writerFactory = context.getProperty(RESULT_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        this.notFoundIsSuccessful = context.getProperty(NOT_FOUND_IS_SUCCESSFUL).asBoolean();

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

        final IndexOperationParameters indexOperationParameters = new IndexOperationParameters(context, input);

        final List<FlowFile> resultRecords = new ArrayList<>();

        final AtomicLong erroredRecords = new AtomicLong(0);
        final AtomicLong successfulRecords = new AtomicLong(0);
        final StopWatch stopWatch = new StopWatch(true);
        final Set<String> indices = new HashSet<>();
        final Set<String> types = new HashSet<>();
        int batches = 0;

        try (final InputStream inStream = session.read(input);
            final RecordReader reader = readerFactory.createRecordReader(input, inStream, getLogger())) {
            final PushBackRecordSet recordSet = new PushBackRecordSet(reader.createRecordSet());
            final List<IndexOperationRequest> operationList = new ArrayList<>();
            final List<Record> originals = new ArrayList<>();

            Record record;
            while ((record = recordSet.next()) != null) {
                addOperation(operationList, record, indexOperationParameters, indices, types);
                originals.add(record);

                if (operationList.size() == indexOperationParameters.getBatchSize() || !recordSet.isAnotherRecord()) {
                    operate(operationList, originals, reader, session, input, indexOperationParameters.getRequestParameters(), resultRecords, erroredRecords, successfulRecords);
                    batches++;
                }
            }

            if (!operationList.isEmpty()) {
                operate(operationList, originals, reader, session, input, indexOperationParameters.getRequestParameters(), resultRecords, erroredRecords, successfulRecords);
                batches++;
            }
        } catch (final ElasticsearchException ese) {
            final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                    ese.isElastic() ? "Routing to retry." : "Routing to failure");
            getLogger().error(msg, ese);
            final Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
            transferFlowFilesOnException(ese, rel, session, true, input);
            removeResultRecordFlowFiles(resultRecords, session);
            return;
        } catch (final IOException | SchemaNotFoundException ex) {
            getLogger().warn("Could not log Elasticsearch operation errors nor determine which documents errored.", ex);
            transferFlowFilesOnException(ex, REL_FAILURE, session, true, input);
            removeResultRecordFlowFiles(resultRecords, session);
            return;
        } catch (final Exception ex) {
            getLogger().error("Could not index documents.", ex);
            transferFlowFilesOnException(ex, REL_FAILURE, session, false, input);
            context.yield();
            removeResultRecordFlowFiles(resultRecords, session);
            return;
        }

        stopWatch.stop();
        session.getProvenanceReporter().send(
                input,
                clientService.get().getTransitUrl(String.join(",", indices), types.isEmpty() ? null : String.join(",", types)),
                String.format(Locale.getDefault(), "%d Elasticsearch _bulk operation batch(es) [%d error(s), %d success(es)]", batches, erroredRecords.get(), successfulRecords.get()),
                stopWatch.getDuration(TimeUnit.MILLISECONDS)
        );

        input = session.putAllAttributes(input, new HashMap<>() {{
            put("elasticsearch.put.error.count", String.valueOf(erroredRecords.get()));
            put("elasticsearch.put.success.count", String.valueOf(successfulRecords.get()));
        }});

        session.transfer(input, REL_SUCCESS);
    }

    private void addOperation(final List<IndexOperationRequest> operationList, final Record record, final IndexOperationParameters indexOperationParameters,
                              final Set<String> indices, final Set<String> types) {
        final String index = getFromRecordPath(record, indexOperationParameters.getIndexPath(), true, indexOperationParameters.getDefaultIndex(), false);
        indices.add(index);
        final String type = getFromRecordPath(record, indexOperationParameters.getTypePath(), true, indexOperationParameters.getDefaultType(), false);
        if (StringUtils.isNotBlank(type)) {
            types.add(type);
        }
        final String op = getFromRecordPath(record, indexOperationParameters.getIndexOpPath(), true, indexOperationParameters.getDefaultIndexOp(), false);
        final IndexOperationRequest.Operation indexOp = IndexOperationRequest.Operation.forValue(op);
        final String id = getFromRecordPath(record, indexOperationParameters.getIdPath(), true, null, indexOperationParameters.isRetainId());
        final Object atTimestamp = getTimestampFromRecordPath(record, indexOperationParameters.getAtTimestampPath(),
                indexOperationParameters.getDefaultAtTimestamp(), indexOperationParameters.isRetainTimestamp());
        final Map<String, Object> script = getMapFromRecordPath(record, indexOperationParameters.getScriptPath());
        final boolean scriptedUpsert = Boolean.parseBoolean(getFromRecordPath(record, indexOperationParameters.getScriptedUpsertPath(), false, "false", false));
        final Map<String, Object> dynamicTemplates = getMapFromRecordPath(record, indexOperationParameters.getDynamicTypesPath());

        final Map<String, String> bulkHeaderFields = new HashMap<>(indexOperationParameters.getBulkHeaderRecordPaths().size(), 1);
        for (final Map.Entry<String, RecordPath> entry : indexOperationParameters.getBulkHeaderRecordPaths().entrySet()) {
            bulkHeaderFields.put(entry.getKey(), getFromRecordPath(record, entry.getValue(), false, null, false));
        }

        @SuppressWarnings("unchecked")
        final Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils
                .convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
        formatDateTimeFields(contentMap, record);
        if (atTimestamp != null) {
            contentMap.putIfAbsent("@timestamp", atTimestamp);
        }

        operationList.add(new IndexOperationRequest(index, type, id, contentMap, indexOp, script, scriptedUpsert, dynamicTemplates, bulkHeaderFields));
    }

    private void operate(final List<IndexOperationRequest> operationList, final List<Record> originals, final RecordReader reader,
                         final ProcessSession session, final FlowFile input, final Map<String, String> requestParameters,
                         final List<FlowFile> resultRecords, final AtomicLong erroredRecords, final AtomicLong successfulRecords)
            throws IOException, SchemaNotFoundException, MalformedRecordException {

        final BulkOperation bundle = new BulkOperation(operationList, originals, reader.getSchema());
        final ResponseDetails responseDetails = indexDocuments(bundle, session, input, requestParameters);

        if (responseDetails.getErrored() != null) {
            resultRecords.add(responseDetails.getErrored());
        }
        erroredRecords.getAndAdd(responseDetails.getErrorCount());

        if (responseDetails.getSucceeded() != null) {
            resultRecords.add(responseDetails.getSucceeded());
        }
        successfulRecords.getAndAdd(responseDetails.getSuccessCount());

        operationList.clear();
        originals.clear();
    }

    private void removeResultRecordFlowFiles(final List<FlowFile> results, final ProcessSession session) {
        for (final FlowFile flowFile : results) {
            session.remove(flowFile);
        }

        results.clear();
    }

    private ResponseDetails indexDocuments(final BulkOperation bundle, final ProcessSession session, final FlowFile input,
                                           final Map<String, String> requestParameters) throws IOException, SchemaNotFoundException {
        final IndexOperationResponse response = clientService.get().bulk(bundle.getOperationList(), requestParameters);

        final Map<Integer, Map<String, Object>> errors = findElasticsearchResponseErrors(response);
        if (!errors.isEmpty()) {
            handleElasticsearchDocumentErrors(errors, session, input);
        }

        final int numErrors = errors.size();
        final int numSuccessful = response.getItems() == null ? 0 : response.getItems().size() - numErrors;
        FlowFile errorFF = null;
        FlowFile successFF = null;

        if (writerFactory != null) {
            try {
                successFF = session.create(input);
                errorFF = session.create(input);

                try (final OutputStream errorOutputStream = session.write(errorFF);
                     final RecordSetWriter errorWriter = writerFactory.createWriter(getLogger(), bundle.getSchema(), errorOutputStream, errorFF);
                     final OutputStream successOutputStream = session.write(successFF);
                     final RecordSetWriter successWriter = writerFactory.createWriter(getLogger(), bundle.getSchema(), successOutputStream, successFF)) {

                    errorWriter.beginRecordSet();
                    successWriter.beginRecordSet();
                    for (int o = 0; o < bundle.getOriginalRecords().size(); o++) {
                        if (errors.containsKey(o)) {
                            errorWriter.write(bundle.getOriginalRecords().get(o));
                        } else {
                            successWriter.write(bundle.getOriginalRecords().get(o));
                        }
                    }
                    errorWriter.finishRecordSet();
                    successWriter.finishRecordSet();
                }

                if (numErrors > 0) {
                    errorFF = session.putAttribute(errorFF, ATTR_RECORD_COUNT, String.valueOf(numErrors));
                    session.transfer(errorFF, REL_FAILED_RECORDS);
                } else {
                    session.remove(errorFF);
                }

                if (numSuccessful > 0) {
                    successFF = session.putAttribute(successFF, ATTR_RECORD_COUNT, String.valueOf(numSuccessful));
                    session.transfer(successFF, REL_SUCCESSFUL_RECORDS);
                } else {
                    session.remove(successFF);
                }
            } catch (final IOException | SchemaNotFoundException ex) {
                getLogger().error("Unable to write error/successful records", ex);
                session.remove(errorFF);
                session.remove(successFF);
                throw ex;
            }
        }

        return new ResponseDetails(successFF, numSuccessful, errorFF, numErrors);
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

    private String getFromRecordPath(final Record record, final RecordPath path, final boolean strictString,
                                     final String fallback, final boolean retain) {
        if (path == null) {
            return fallback;
        }

        final RecordPathResult result = path.evaluate(record);
        final Optional<FieldValue> value = result.getSelectedFields().findFirst();
        if (value.isPresent() && value.get().getValue() != null && DataTypeUtils.isStringTypeCompatible(value.get().getValue())) {
            final FieldValue fieldValue = value.get();
            if (strictString && !fieldValue.getField().getDataType().getFieldType().equals(RecordFieldType.STRING)) {
                throw new ProcessException(
                    String.format("Field referenced by %s must be a string", path.getPath())
                );
            }

            if (!retain) {
                fieldValue.updateValue(null);
            }

            return fieldValue.toString();
        } else {
            return fallback;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMapFromRecordPath(final Record record, final RecordPath path) {
        if (path == null) {
            return Collections.emptyMap();
        }

        final RecordPathResult result = path.evaluate(record);
        final Optional<FieldValue> value = result.getSelectedFields().findFirst();
        if (value.isPresent() && value.get().getValue() != null) {
            final FieldValue fieldValue = value.get();
            final Map<String, Object> map;
            if (DataTypeUtils.isMapTypeCompatible(fieldValue.getValue())) {
                map = DataTypeUtils.toMap(fieldValue.getValue(), path.getPath());
            } else {
                try {
                    map = MAPPER.readValue(fieldValue.getValue().toString(), Map.class);
                } catch (final JsonProcessingException jpe) {
                    getLogger().error("Unable to parse field {} as Map", path.getPath(), jpe);
                    throw new ProcessException(
                            String.format("Field referenced by %s must be Map-type compatible or a String parsable into a JSON Object", path.getPath())
                    );
                }
            }

            fieldValue.updateValue(null);

            return map;
        } else {
            return Collections.emptyMap();
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

    private static class ResponseDetails {
        final FlowFile errored;
        final FlowFile succeeded;
        final int errorCount;
        final int successCount;

        ResponseDetails(final FlowFile succeeded, final int successCount, final FlowFile errored, final int errorCount) {
            this.succeeded = succeeded;
            this.successCount = successCount;
            this.errored = errored;
            this.errorCount = errorCount;
        }

        public FlowFile getSucceeded() {
            return succeeded;
        }

        public FlowFile getErrored() {
            return errored;
        }

        public int getErrorCount() {
            return errorCount;
        }

        public int getSuccessCount() {
            return successCount;
        }
    }

    private class IndexOperationParameters {
        private final String defaultIndexOp;
        private final String defaultIndex;
        private final String defaultType;
        private final String defaultAtTimestamp;

        private final RecordPath indexOpPath;
        private final RecordPath idPath;
        private final RecordPath indexPath;
        private final RecordPath typePath;
        private final RecordPath atTimestampPath;
        private final RecordPath scriptPath;

        private final RecordPath scriptedUpsertPath;
        private final RecordPath dynamicTypesPath;

        private final Map<String, String> requestParameters;
        private final Map<String, RecordPath> bulkHeaderRecordPaths;

        private final boolean retainId;
        private final boolean retainTimestamp;
        private final int batchSize;

        IndexOperationParameters(final ProcessContext context, final FlowFile input) {
            defaultIndexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(input).getValue();
            defaultIndex = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
            defaultType = context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue();
            defaultAtTimestamp = context.getProperty(AT_TIMESTAMP).evaluateAttributeExpressions(input).getValue();

            indexOpPath = compileRecordPathFromProperty(context, INDEX_OP_RECORD_PATH, input);
            idPath = compileRecordPathFromProperty(context, ID_RECORD_PATH, input);
            indexPath = compileRecordPathFromProperty(context, INDEX_RECORD_PATH, input);
            typePath = compileRecordPathFromProperty(context, TYPE_RECORD_PATH, input);
            atTimestampPath = compileRecordPathFromProperty(context, AT_TIMESTAMP_RECORD_PATH, input);
            scriptPath = compileRecordPathFromProperty(context, SCRIPT_RECORD_PATH, input);
            scriptedUpsertPath = compileRecordPathFromProperty(context, SCRIPTED_UPSERT_RECORD_PATH, input);
            dynamicTypesPath = compileRecordPathFromProperty(context, DYNAMIC_TEMPLATES_RECORD_PATH, input);

            final Map<String, String> dynamicProperties = getDynamicProperties(context, input);
            requestParameters = getRequestURLParameters(dynamicProperties);

            final Map<String, String> bulkHeaderParameterPaths = getBulkHeaderParameters(dynamicProperties);
            bulkHeaderRecordPaths = new HashMap<>(bulkHeaderParameterPaths.size(), 1);
            for (final Map.Entry<String, String> entry : bulkHeaderParameterPaths.entrySet()) {
                if (StringUtils.isNotBlank(entry.getValue())) {
                    final RecordPath rp = recordPathCache.getCompiled(entry.getValue());
                    if (rp != null) {
                        bulkHeaderRecordPaths.put(entry.getKey(), rp);
                    }
                }
            }

            retainId = context.getProperty(RETAIN_ID_FIELD).evaluateAttributeExpressions(input).asBoolean();
            retainTimestamp = context.getProperty(RETAIN_AT_TIMESTAMP_FIELD).evaluateAttributeExpressions(input).asBoolean();
            batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(input).asInteger();
        }

        private RecordPath compileRecordPathFromProperty(final ProcessContext context, final PropertyDescriptor property, final FlowFile input) {
            final String recordPathProperty = context.getProperty(property).evaluateAttributeExpressions(input).getValue();
            return StringUtils.isNotBlank(recordPathProperty) ? recordPathCache.getCompiled(recordPathProperty) : null;
        }

        public String getDefaultIndexOp() {
            return defaultIndexOp;
        }

        public String getDefaultIndex() {
            return defaultIndex;
        }

        public String getDefaultType() {
            return defaultType;
        }

        public String getDefaultAtTimestamp() {
            return defaultAtTimestamp;
        }

        public RecordPath getIndexOpPath() {
            return indexOpPath;
        }

        public RecordPath getIdPath() {
            return idPath;
        }

        public RecordPath getIndexPath() {
            return indexPath;
        }

        public RecordPath getTypePath() {
            return typePath;
        }

        public RecordPath getAtTimestampPath() {
            return atTimestampPath;
        }

        public RecordPath getScriptPath() {
            return scriptPath;
        }

        public RecordPath getScriptedUpsertPath() {
            return scriptedUpsertPath;
        }

        public RecordPath getDynamicTypesPath() {
            return dynamicTypesPath;
        }

        public Map<String, String> getRequestParameters() {
            return requestParameters;
        }

        public Map<String, RecordPath> getBulkHeaderRecordPaths() {
            return bulkHeaderRecordPaths;
        }

        public boolean isRetainId() {
            return retainId;
        }

        public boolean isRetainTimestamp() {
            return retainTimestamp;
        }

        public int getBatchSize() {
            return batchSize;
        }
    }
}
