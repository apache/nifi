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

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"elasticsearch", "insert", "update", "upsert", "delete", "write", "put", "http", "record"})
@CapabilityDescription("Writes the records from a FlowFile into to Elasticsearch, using the specified parameters such as "
        + "the index to insert into and the type of the document, as well as the operation type (index, upsert, delete, etc.). Note: The Bulk API is used to "
        + "send the records. This means that the entire contents of the incoming flow file are read into memory, and each record is transformed into a JSON document "
        + "which is added to a single HTTP request body. For very large flow files (files with a large number of records, e.g.), this could cause memory usage issues.")
@DynamicProperty(
        name = "A URL query parameter",
        value = "The value to set it to",
        supportsExpressionLanguage = true,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing")
public class PutElasticsearchHttpRecord extends AbstractElasticsearchHttpProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("put-es-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor ID_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("put-es-record-id-path")
            .displayName("Identifier Record Path")
            .description("A RecordPath pointing to a field in the record(s) that contains the identifier for the document. If the Index Operation is \"index\", "
                    + "this property may be left empty or evaluate to an empty value, in which case the document's identifier will be "
                    + "auto-generated by Elasticsearch. For all other Index Operations, the field's value must be non-empty.")
            .required(false)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("put-es-record-index")
            .displayName("Index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("put-es-record-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    static final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
            .name("put-es-record-index-op")
            .displayName("Index Operation")
            .description("The type of the operation used to index (index, update, upsert, delete)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("index")
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    private volatile RecordPathCache recordPathCache;

    private final JsonFactory factory = new JsonFactory();

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ES_URL);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CONNECT_TIMEOUT);
        descriptors.add(RESPONSE_TIMEOUT);
        descriptors.add(RECORD_READER);
        descriptors.add(ID_RECORD_PATH);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(INDEX_OP);

        propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));
        // Since Expression Language is allowed for index operation, we can't guarantee that we can catch
        // all invalid configurations, but we should catch them as soon as we can. For example, if the
        // Identifier Record Path property is empty, the Index Operation must evaluate to "index".
        String idPath = validationContext.getProperty(ID_RECORD_PATH).getValue();
        String indexOp = validationContext.getProperty(INDEX_OP).getValue();

        if (StringUtils.isEmpty(idPath)) {
            switch (indexOp.toLowerCase()) {
                case "update":
                case "upsert":
                case "delete":
                case "":
                    problems.add(new ValidationResult.Builder()
                            .valid(false)
                            .subject(INDEX_OP.getDisplayName())
                            .explanation("If Identifier Record Path is not set, Index Operation must evaluate to \"index\"")
                            .build());
                    break;
                default:
                    break;
            }
        }
        return problems;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        super.setup(context);
        recordPathCache = new RecordPathCache(10);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        // Authentication
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(flowFile).getValue();

        OkHttpClient okHttpClient = getClient();
        final ComponentLog logger = getLogger();

        final String baseUrl = trimToEmpty(context.getProperty(ES_URL).evaluateAttributeExpressions().getValue());
        HttpUrl.Builder urlBuilder = HttpUrl.parse(baseUrl).newBuilder().addPathSegment("_bulk");

        // Find the user-added properties and set them as query parameters on the URL
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            PropertyDescriptor pd = property.getKey();
            if (pd.isDynamic()) {
                if (property.getValue() != null) {
                    urlBuilder = urlBuilder.addQueryParameter(pd.getName(), context.getProperty(pd).evaluateAttributeExpressions().getValue());
                }
            }
        }
        final URL url = urlBuilder.build().url();

        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(index)) {
            logger.error("No value for index in for {}, transferring to failure", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile).getValue();
        String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(indexOp)) {
            logger.error("No Index operation specified for {}, transferring to failure.", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        switch (indexOp.toLowerCase()) {
            case "index":
            case "update":
            case "upsert":
            case "delete":
                break;
            default:
                logger.error("Index operation {} not supported for {}, transferring to failure.", new Object[]{indexOp, flowFile});
                session.transfer(flowFile, REL_FAILURE);
                return;
        }

        final String id_path = context.getProperty(ID_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final RecordPath recordPath = StringUtils.isEmpty(id_path) ? null : recordPathCache.getCompiled(id_path);
        final StringBuilder sb = new StringBuilder();

        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            Record record;
            while ((record = reader.nextRecord()) != null) {

                final String id;
                if (recordPath != null) {
                    Optional<FieldValue> idPathValue = recordPath.evaluate(record).getSelectedFields().findFirst();
                    if (!idPathValue.isPresent() || idPathValue.get().getValue() == null) {
                        throw new IdentifierNotFoundException("Identifier Record Path specified but no value was found, transferring {} to failure.");
                    }
                    id = idPathValue.get().getValue().toString();
                } else {
                    id = null;
                }

                // The ID must be valid for all operations except "index". For that case,
                // a missing ID indicates one is to be auto-generated by Elasticsearch
                if (id == null && !indexOp.equalsIgnoreCase("index")) {
                    throw new IdentifierNotFoundException("Index operation {} requires a valid identifier value from a flow file attribute, transferring to failure.");
                }

                final StringBuilder json = new StringBuilder();

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                JsonGenerator generator = factory.createJsonGenerator(out);
                writeRecord(record, record.getSchema(), generator);
                generator.flush();
                generator.close();
                json.append(out.toString());

                if (indexOp.equalsIgnoreCase("index")) {
                    sb.append("{\"index\": { \"_index\": \"");
                    sb.append(index);
                    sb.append("\", \"_type\": \"");
                    sb.append(docType);
                    sb.append("\"");
                    if (!StringUtils.isEmpty(id)) {
                        sb.append(", \"_id\": \"");
                        sb.append(id);
                        sb.append("\"");
                    }
                    sb.append("}}\n");
                    sb.append(json);
                    sb.append("\n");
                } else if (indexOp.equalsIgnoreCase("upsert") || indexOp.equalsIgnoreCase("update")) {
                    sb.append("{\"update\": { \"_index\": \"");
                    sb.append(index);
                    sb.append("\", \"_type\": \"");
                    sb.append(docType);
                    sb.append("\", \"_id\": \"");
                    sb.append(id);
                    sb.append("\" }\n");
                    sb.append("{\"doc\": ");
                    sb.append(json);
                    sb.append(", \"doc_as_upsert\": ");
                    sb.append(indexOp.equalsIgnoreCase("upsert"));
                    sb.append(" }\n");
                } else if (indexOp.equalsIgnoreCase("delete")) {
                    sb.append("{\"delete\": { \"_index\": \"");
                    sb.append(index);
                    sb.append("\", \"_type\": \"");
                    sb.append(docType);
                    sb.append("\", \"_id\": \"");
                    sb.append(id);
                    sb.append("\" }\n");
                }
            }
        } catch (IdentifierNotFoundException infe) {
            logger.error(infe.getMessage(), new Object[]{flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;

        } catch (final IOException | SchemaNotFoundException | MalformedRecordException e) {
            logger.error("Could not parse incoming data", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), sb.toString());
        final Response getResponse;
        try {
            getResponse = sendRequestToElasticsearch(okHttpClient, url, username, password, "PUT", requestBody);
        } catch (final Exception e) {
            logger.error("Routing to {} due to exception: {}", new Object[]{REL_FAILURE.getName(), e}, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        final int statusCode = getResponse.code();

        if (isSuccess(statusCode)) {
            ResponseBody responseBody = getResponse.body();
            try {
                final byte[] bodyBytes = responseBody.bytes();

                JsonNode responseJson = parseJsonResponse(new ByteArrayInputStream(bodyBytes));
                boolean errors = responseJson.get("errors").asBoolean(false);
                // ES has no rollback, so if errors occur, log them and route the whole flow file to failure
                if (errors) {
                    ArrayNode itemNodeArray = (ArrayNode) responseJson.get("items");
                    if (itemNodeArray.size() > 0) {
                        // All items are returned whether they succeeded or failed, so iterate through the item array
                        // at the same time as the flow file list, logging failures accordingly
                        for (int i = itemNodeArray.size() - 1; i >= 0; i--) {
                            JsonNode itemNode = itemNodeArray.get(i);
                            int status = itemNode.findPath("status").asInt();
                            if (!isSuccess(status)) {
                                String reason = itemNode.findPath("//error/reason").asText();
                                logger.error("Failed to insert {} into Elasticsearch due to {}, transferring to failure",
                                        new Object[]{flowFile, reason});
                            }
                        }
                    }
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().send(flowFile, url.toString());
                }

            } catch (IOException ioe) {
                // Something went wrong when parsing the response, log the error and route to failure
                logger.error("Error parsing Bulk API response: {}", new Object[]{ioe.getMessage()}, ioe);
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            }
        } else if (statusCode / 100 == 5) {
            // 5xx -> RETRY, but a server error might last a while, so yield
            logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to retry. This is likely a server problem, yielding...",
                    new Object[]{statusCode, getResponse.message()});
            session.transfer(flowFile, REL_RETRY);
            context.yield();
        } else {  // 1xx, 3xx, 4xx, etc. -> NO RETRY
            logger.warn("Elasticsearch returned code {} with message {}, transferring flow file to failure", new Object[]{statusCode, getResponse.message()});
            session.transfer(flowFile, REL_FAILURE);
        }
        getResponse.close();
    }

    private void writeRecord(final Record record, final RecordSchema writeSchema, final JsonGenerator generator)
            throws IOException {
        RecordSchema schema = record.getSchema();

        generator.writeStartObject();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            final RecordField field = schema.getField(i);
            final String fieldName = field.getFieldName();
            final Object value = record.getValue(field);
            if (value == null) {
                generator.writeNullField(fieldName);
                continue;
            }

            generator.writeFieldName(fieldName);
            final DataType dataType = schema.getDataType(fieldName).get();

            writeValue(generator, value, fieldName, dataType);
        }
        generator.writeEndObject();
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final JsonGenerator generator, final Object value, final String fieldName, final DataType dataType) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        final Object coercedValue = DataTypeUtils.convertType(value, chosenDataType, fieldName);
        if (coercedValue == null) {
            generator.writeNull();
            return;
        }

        switch (chosenDataType.getFieldType()) {
            case DATE: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(RecordFieldType.DATE.getDefaultFormat()));
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIME: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(RecordFieldType.TIME.getDefaultFormat()));
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIMESTAMP: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(RecordFieldType.TIMESTAMP.getDefaultFormat()));
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case DOUBLE:
                generator.writeNumber(DataTypeUtils.toDouble(coercedValue, fieldName));
                break;
            case FLOAT:
                generator.writeNumber(DataTypeUtils.toFloat(coercedValue, fieldName));
                break;
            case LONG:
                generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                break;
            case INT:
            case BYTE:
            case SHORT:
                generator.writeNumber(DataTypeUtils.toInteger(coercedValue, fieldName));
                break;
            case CHAR:
            case STRING:
                generator.writeString(coercedValue.toString());
                break;
            case BIGINT:
                if (coercedValue instanceof Long) {
                    generator.writeNumber((Long) coercedValue);
                } else {
                    generator.writeNumber((BigInteger) coercedValue);
                }
                break;
            case BOOLEAN:
                final String stringValue = coercedValue.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(true);
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(false);
                } else {
                    generator.writeString(stringValue);
                }
                break;
            case RECORD: {
                final Record record = (Record) coercedValue;
                final RecordDataType recordDataType = (RecordDataType) chosenDataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();
                writeRecord(record, childSchema, generator);
                break;
            }
            case MAP: {
                final MapDataType mapDataType = (MapDataType) chosenDataType;
                final DataType valueDataType = mapDataType.getValueType();
                final Map<String, ?> map = (Map<String, ?>) coercedValue;
                generator.writeStartObject();
                for (final Map.Entry<String, ?> entry : map.entrySet()) {
                    final String mapKey = entry.getKey();
                    final Object mapValue = entry.getValue();
                    generator.writeFieldName(mapKey);
                    writeValue(generator, mapValue, fieldName + "." + mapKey, valueDataType);
                }
                generator.writeEndObject();
                break;
            }
            case ARRAY:
            default:
                if (coercedValue instanceof Object[]) {
                    final Object[] values = (Object[]) coercedValue;
                    final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                    final DataType elementType = arrayDataType.getElementType();
                    writeArray(values, fieldName, generator, elementType);
                } else {
                    generator.writeString(coercedValue.toString());
                }
                break;
        }
    }

    private void writeArray(final Object[] values, final String fieldName, final JsonGenerator generator, final DataType elementType) throws IOException {
        generator.writeStartArray();
        for (final Object element : values) {
            writeValue(generator, element, fieldName, elementType);
        }
        generator.writeEndArray();
    }
}

