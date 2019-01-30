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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
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
import org.apache.nifi.serialization.SimpleDateFormatValidator;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"elasticsearch", "insert", "update", "upsert", "delete", "write", "put", "http", "record"})
@CapabilityDescription("Writes the records from a FlowFile into to Elasticsearch, using the specified parameters such as "
        + "the index to insert into and the type of the document, as well as the operation type (index, upsert, delete, etc.). Note: The Bulk API is used to "
        + "send the records. This means that the entire contents of the incoming flow file are read into memory, and each record is transformed into a JSON document "
        + "which is added to a single HTTP request body. For very large flow files (files with a large number of records, e.g.), this could cause memory usage issues.")
@WritesAttributes({
        @WritesAttribute(attribute="record.count", description="The number of records in an outgoing FlowFile. This is only populated on the 'success' relationship."),
        @WritesAttribute(attribute="failure.count", description="The number of records found by Elasticsearch to have errors. This is only populated on the 'failure' relationship.")
})
@DynamicProperty(
        name = "A URL query parameter",
        value = "The value to set it to",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
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
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("put-es-record-index")
            .displayName("Index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("put-es-record-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    static final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
            .name("put-es-record-index-op")
            .displayName("Index Operation")
            .description("The type of the operation used to index (index, update, upsert, delete)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("index")
            .build();

    static final AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Always Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will not be written out");

    static final AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Never Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null, will be written out as a null value");

    static final AllowableValue SUPPRESS_MISSING = new AllowableValue("suppress-missing", "Suppress Missing Values",
            "When a field has a value of null, it will be written out. However, if a field is defined in the schema and not present in the record, the field will not be written out.");

    static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("suppress-nulls")
            .displayName("Suppress Null Values")
            .description("Specifies how the writer should handle a null field")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS, SUPPRESS_MISSING)
            .defaultValue(NEVER_SUPPRESS.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor.Builder()
            .name("Date Format")
            .description("Specifies the format to use when reading/writing Date fields. "
                    + "If not specified, the default format '" + RecordFieldType.DATE.getDefaultFormat() + "' is used. "
                    + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy for a two-digit month, followed by "
                    + "a two-digit day, followed by a four-digit year, all separated by '/' characters, as in 01/01/2017).")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(new SimpleDateFormatValidator())
            .required(false)
            .build();
    static final PropertyDescriptor TIME_FORMAT = new PropertyDescriptor.Builder()
            .name("Time Format")
            .description("Specifies the format to use when reading/writing Time fields. "
                    + "If not specified, the default format '" + RecordFieldType.TIME.getDefaultFormat() + "' is used. "
                    + "If specified, the value must match the Java Simple Date Format (for example, HH:mm:ss for a two-digit hour in 24-hour format, followed by "
                    + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 18:04:15).")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(new SimpleDateFormatValidator())
            .required(false)
            .build();
    static final PropertyDescriptor TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
            .name("Timestamp Format")
            .description("Specifies the format to use when reading/writing Timestamp fields. "
                    + "If not specified, the default format '" + RecordFieldType.TIMESTAMP.getDefaultFormat() + "' is used. "
                    + "If specified, the value must match the Java Simple Date Format (for example, MM/dd/yyyy HH:mm:ss for a two-digit month, followed by "
                    + "a two-digit day, followed by a four-digit year, all separated by '/' characters; and then followed by a two-digit hour in 24-hour format, followed by "
                    + "a two-digit minute, followed by a two-digit second, all separated by ':' characters, as in 01/01/2017 18:04:15).")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(new SimpleDateFormatValidator())
            .required(false)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    private volatile RecordPathCache recordPathCache;

    private final JsonFactory factory = new JsonFactory();

    private volatile String nullSuppression;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>(COMMON_PROPERTY_DESCRIPTORS);
        descriptors.add(RECORD_READER);
        descriptors.add(ID_RECORD_PATH);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(CHARSET);
        descriptors.add(INDEX_OP);
        descriptors.add(SUPPRESS_NULLS);
        descriptors.add(DATE_FORMAT);
        descriptors.add(TIME_FORMAT);
        descriptors.add(TIMESTAMP_FORMAT);

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

        final String baseUrl = context.getProperty(ES_URL).evaluateAttributeExpressions().getValue().trim();
        if (StringUtils.isEmpty(baseUrl)) {
            throw new ProcessException("Elasticsearch URL is empty or null, this indicates an invalid Expression (missing variables, e.g.)");
        }
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

        this.nullSuppression = context.getProperty(SUPPRESS_NULLS).getValue();

        final String id_path = context.getProperty(ID_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final RecordPath recordPath = StringUtils.isEmpty(id_path) ? null : recordPathCache.getCompiled(id_path);
        final StringBuilder sb = new StringBuilder();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        int recordCount = 0;
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
                json.append(out.toString(charset.name()));

                buildBulkCommand(sb, index, docType, indexOp, id, json.toString());
                recordCount++;
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
                int failureCount = 0;
                // ES has no rollback, so if errors occur, log them and route the whole flow file to failure
                if (errors) {
                    ArrayNode itemNodeArray = (ArrayNode) responseJson.get("items");
                    if(itemNodeArray != null) {
                        if (itemNodeArray.size() > 0) {
                            // All items are returned whether they succeeded or failed, so iterate through the item array
                            // at the same time as the flow file list, moving each to success or failure accordingly,
                            // but only keep the first error for logging
                            String errorReason = null;
                            for (int i = itemNodeArray.size() - 1; i >= 0; i--) {
                                JsonNode itemNode = itemNodeArray.get(i);
                                int status = itemNode.findPath("status").asInt();
                                if (!isSuccess(status)) {
                                    if (errorReason == null) {
                                        // Use "result" if it is present; this happens for status codes like 404 Not Found, which may not have an error/reason
                                        String reason = itemNode.findPath("result").asText();
                                        if (StringUtils.isEmpty(reason)) {
                                            // If there was no result, we expect an error with a string description in the "reason" field
                                            reason = itemNode.findPath("reason").asText();
                                        }
                                        errorReason = reason;
                                        logger.error("Failed to process {} due to {}, transferring to failure",
                                                new Object[]{flowFile, errorReason});
                                    }
                                    failureCount++;
                                }
                            }
                        }
                    }
                    flowFile = session.putAttribute(flowFile, "failure.count", Integer.toString(failureCount));
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    flowFile = session.putAttribute(flowFile, "record.count", Integer.toString(recordCount));
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
                if (nullSuppression.equals(NEVER_SUPPRESS.getValue()) || (nullSuppression.equals(SUPPRESS_MISSING.getValue())) && record.getRawFieldNames().contains(fieldName)) {
                    generator.writeNullField(fieldName);
                }

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
            if (nullSuppression.equals(NEVER_SUPPRESS.getValue()) || ((nullSuppression.equals(SUPPRESS_MISSING.getValue())) && fieldName != null && !fieldName.equals(""))) {
                generator.writeNullField(fieldName);
            }

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
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(this.dateFormat));
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIME: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(this.timeFormat));
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIMESTAMP: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(this.timestampFormat));
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
                    final ArrayDataType arrayDataType = (ArrayDataType) chosenDataType;
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

