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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

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
@Tags({"json", "elasticsearch", "elasticsearch5", "elasticsearch6", "put", "index", "record"})
@CapabilityDescription("A record-aware Elasticsearch put processor that uses the official Elastic REST client libraries.")
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
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .allowableValues(
                    IndexOperationRequest.Operation.Create.getValue(),
                    IndexOperationRequest.Operation.Delete.getValue(),
                    IndexOperationRequest.Operation.Index.getValue(),
                    IndexOperationRequest.Operation.Update.getValue(),
                    IndexOperationRequest.Operation.Upsert.getValue()
            )
            .defaultValue(IndexOperationRequest.Operation.Index.getValue())
            .build();

    static final PropertyDescriptor INDEX_OP_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("put-es-record-index-op-path")
            .displayName("Index Operation Record Path")
            .description("A record path expression to retrieve the Index Operation field for use with Elasticsearch. If left blank " +
                    "the Index Operation will be determined using the main Index Operation property.")
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor ID_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("put-es-record-id-path")
        .displayName("ID Record Path")
        .description("A record path expression to retrieve the ID field for use with Elasticsearch. If left blank " +
                "the ID will be automatically generated by Elasticsearch.")
        .addValidator(new RecordPathValidator())
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

    static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        INDEX_OP, INDEX, TYPE, CLIENT_SERVICE, RECORD_READER, BATCH_SIZE, ID_RECORD_PATH, INDEX_OP_RECORD_PATH,
        INDEX_RECORD_PATH, TYPE_RECORD_PATH, LOG_ERROR_RESPONSES, ERROR_RECORD_WRITER
    ));
    static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_RETRY, REL_FAILED_RECORDS
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private RecordReaderFactory readerFactory;
    private RecordPathCache recordPathCache;
    private ElasticSearchClientService clientService;
    private RecordSetWriterFactory writerFactory;
    private boolean logErrors;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        this.clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
        this.recordPathCache = new RecordPathCache(16);
        this.writerFactory = context.getProperty(ERROR_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        this.logErrors = context.getProperty(LOG_ERROR_RESPONSES).asBoolean();
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(input).getValue();
        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
        final String type  = context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue();

        final String indexOpPath = context.getProperty(INDEX_OP_RECORD_PATH).isSet()
                ? context.getProperty(INDEX_OP_RECORD_PATH).evaluateAttributeExpressions(input).getValue()
                : null;
        final String idPath = context.getProperty(ID_RECORD_PATH).isSet()
                ? context.getProperty(ID_RECORD_PATH).evaluateAttributeExpressions(input).getValue()
                : null;
        final String indexPath = context.getProperty(INDEX_RECORD_PATH).isSet()
                ? context.getProperty(INDEX_RECORD_PATH).evaluateAttributeExpressions(input).getValue()
                : null;
        final String typePath = context.getProperty(TYPE_RECORD_PATH).isSet()
                ? context.getProperty(TYPE_RECORD_PATH).evaluateAttributeExpressions(input).getValue()
                : null;

        RecordPath ioPath = indexOpPath != null ? recordPathCache.getCompiled(indexOpPath) : null;
        RecordPath path = idPath != null ? recordPathCache.getCompiled(idPath) : null;
        RecordPath iPath = indexPath != null ? recordPathCache.getCompiled(indexPath) : null;
        RecordPath tPath = typePath != null ? recordPathCache.getCompiled(typePath) : null;

        int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(input).asInteger();
        List<FlowFile> badRecords = new ArrayList<>();

        try (final InputStream inStream = session.read(input);
             final RecordReader reader = readerFactory.createRecordReader(input, inStream, getLogger())) {
            Record record;
            List<IndexOperationRequest> operationList = new ArrayList<>();
            List<Record> originals = new ArrayList<>();

            while ((record = reader.nextRecord()) != null) {
                final String idx = getFromRecordPath(record, iPath, index);
                final String t   = getFromRecordPath(record, tPath, type);
                final IndexOperationRequest.Operation o = IndexOperationRequest.Operation.forValue(getFromRecordPath(record, ioPath, indexOp));
                final String id  = path != null ? getFromRecordPath(record, path, null) : null;

                Map<String, Object> contentMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));

                operationList.add(new IndexOperationRequest(idx, t, id, contentMap, o));
                originals.add(record);

                if (operationList.size() == batchSize) {
                    BulkOperation bundle = new BulkOperation(operationList, originals, reader.getSchema());
                    FlowFile bad = indexDocuments(bundle, session, input);
                    if (bad != null) {
                        badRecords.add(bad);
                    }

                    operationList.clear();
                    originals.clear();
                }
            }

            if (operationList.size() > 0) {
                BulkOperation bundle = new BulkOperation(operationList, originals, reader.getSchema());
                FlowFile bad = indexDocuments(bundle, session, input);
                if (bad != null) {
                    badRecords.add(bad);
                }
            }
        } catch (ElasticsearchError ese) {
            String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                    ese.isElastic() ? "Moving to retry." : "Moving to failure");
            getLogger().error(msg, ese);
            Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
            session.penalize(input);
            session.transfer(input, rel);
            removeBadRecordFlowFiles(badRecords, session);
            return;
        } catch (Exception ex) {
            getLogger().error("Could not index documents.", ex);
            session.transfer(input, REL_FAILURE);
            removeBadRecordFlowFiles(badRecords, session);
            return;
        }
        session.transfer(input, REL_SUCCESS);
    }

    private void removeBadRecordFlowFiles(List<FlowFile> bad, ProcessSession session) {
        for (FlowFile badFlowFile : bad) {
            session.remove(badFlowFile);
        }

        bad.clear();
    }

    private FlowFile indexDocuments(BulkOperation bundle, ProcessSession session, FlowFile input) throws Exception {
        IndexOperationResponse response = clientService.bulk(bundle.getOperationList());
        if (response.hasErrors()) {
            if(logErrors || getLogger().isDebugEnabled()) {
                List<Map<String, Object>> errors = response.getItems();
                ObjectMapper mapper = new ObjectMapper();
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                String output = String.format("An error was encountered while processing bulk operations. Server response below:\n\n%s", mapper.writeValueAsString(errors));

                if (logErrors) {
                    getLogger().error(output);
                } else {
                    getLogger().debug(output);
                }
            }

            if (writerFactory != null) {
                FlowFile errorFF = session.create(input);
                try (OutputStream os = session.write(errorFF);
                     RecordSetWriter writer = writerFactory.createWriter(getLogger(), bundle.getSchema(), os )) {

                    int added = 0;
                    writer.beginRecordSet();
                    for (int index = 0; index < response.getItems().size(); index++) {
                        Map<String, Object> current = response.getItems().get(index);
                        String key = current.keySet().stream().findFirst().get();
                        Map<String, Object> inner = (Map<String, Object>) current.get(key);
                        if (inner.containsKey("error")) {
                            writer.write(bundle.getOriginalRecords().get(index));
                            added++;
                        }
                    }
                    writer.finishRecordSet();
                    writer.close();
                    os.close();

                    errorFF = session.putAttribute(errorFF, ATTR_RECORD_COUNT, String.valueOf(added));

                    session.transfer(errorFF, REL_FAILED_RECORDS);

                    return errorFF;
                } catch (Exception ex) {
                    getLogger().error("", ex);
                    session.remove(errorFF);
                    throw ex;
                }
            }

            return null;
        } else {
            return null;
        }
    }

    private String getFromRecordPath(Record record, RecordPath path, final String fallback) {
        if (path == null) {
            return fallback;
        }

        RecordPathResult result = path.evaluate(record);
        Optional<FieldValue> value = result.getSelectedFields().findFirst();
        if (value.isPresent() && value.get().getValue() != null) {
            FieldValue fieldValue = value.get();
            if (!fieldValue.getField().getDataType().getFieldType().equals(RecordFieldType.STRING) ) {
                throw new ProcessException(
                    String.format("Field referenced by %s must be a string.", path.getPath())
                );
            }

            fieldValue.updateValue(null);

            String retVal = fieldValue.getValue().toString();

            return retVal;
        } else {
            return fallback;
        }
    }
}
