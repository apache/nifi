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
package org.apache.nifi.reporting;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.reporting.s2s.SiteToSiteUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import javax.json.JsonArray;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Base class for ReportingTasks that send data over site-to-site.
 */
public abstract class AbstractSiteToSiteReportingTask extends AbstractReportingTask implements VerifiableReportingTask {
    private static final String ESTABLISH_COMMUNICATION = "Establish Site-to-Site Connection";

    protected static final String LAST_EVENT_ID_KEY = "last_event_id";
    protected static final String DESTINATION_URL_PATH = "/nifi";
    protected static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    protected static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT).withZone(ZoneOffset.UTC);

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(false)
            .build();

    static final PropertyDescriptor ALLOW_NULL_VALUES = new PropertyDescriptor.Builder()
            .name("include-null-values")
            .displayName("Include Null Values")
            .description("Indicate if null values should be included in records. Default will be false")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    protected volatile SiteToSiteClient siteToSiteClient;
    protected volatile RecordSchema recordSchema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SiteToSiteUtils.DESTINATION_URL);
        properties.add(SiteToSiteUtils.PORT_NAME);
        properties.add(SiteToSiteUtils.SSL_CONTEXT);
        properties.add(SiteToSiteUtils.INSTANCE_URL);
        properties.add(SiteToSiteUtils.COMPRESS);
        properties.add(SiteToSiteUtils.TIMEOUT);
        properties.add(SiteToSiteUtils.BATCH_SIZE);
        properties.add(SiteToSiteUtils.TRANSPORT_PROTOCOL);
        properties.add(SiteToSiteUtils.PROXY_CONFIGURATION_SERVICE);
        properties.add(RECORD_WRITER);
        properties.add(ALLOW_NULL_VALUES);
        return properties;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        ProxyServiceMigration.migrateProxyProperties(config, SiteToSiteUtils.PROXY_CONFIGURATION_SERVICE,
                SiteToSiteUtils.OBSOLETE_PROXY_HOST, SiteToSiteUtils.OBSOLETE_PROXY_PORT, SiteToSiteUtils.OBSOLETE_PROXY_USERNAME, SiteToSiteUtils.OBSOLETE_PROXY_PASSWORD);
    }

    public void setup(final PropertyContext reportContext) {
        if (siteToSiteClient == null) {
            siteToSiteClient = SiteToSiteUtils.getClient(reportContext, getLogger(), null);
        }
    }

    @OnStopped
    public void shutdown() throws IOException {
        final SiteToSiteClient client = getClient();
        if (client != null) {
            client.close();
            siteToSiteClient = null;
        }
    }

    // this getter is intended explicitly for testing purposes
    protected SiteToSiteClient getClient() {
        return this.siteToSiteClient;
    }

    protected void sendData(final ReportingContext context, final Transaction transaction, Map<String, String> attributes, final JsonArray jsonArray) throws IOException {
        if (context.getProperty(RECORD_WRITER).isSet()) {
            transaction.send(getData(context, new ByteArrayInputStream(jsonArray.toString().getBytes(StandardCharsets.UTF_8)), attributes), attributes);
        } else {
            transaction.send(jsonArray.toString().getBytes(StandardCharsets.UTF_8), attributes);
        }
    }

    protected byte[] getData(final ReportingContext context, InputStream in, Map<String, String> attributes) {
        try (final JsonRecordReader reader = new JsonRecordReader(in, recordSchema)) {

            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
            final RecordSchema writeSchema = writerFactory.getSchema(null, recordSchema);
            final ByteArrayOutputStream out = new ByteArrayOutputStream();

            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, attributes)) {
                writer.beginRecordSet();

                Record record;
                while ((record = reader.nextRecord()) != null) {
                    writer.write(record);
                }

                final WriteResult writeResult = writer.finishRecordSet();

                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());
            }

            return out.toByteArray();
        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            throw new ProcessException("Failed to write metrics using record writer: " + e.getMessage(), e);
        }
    }

    protected void addField(final JsonObjectBuilder builder, final String key, final Boolean value, final boolean allowNullValues) {
        if (value != null) {
            builder.add(key, value);
        } else if (allowNullValues) {
            builder.add(key, JsonValue.NULL);
        }
    }

    protected void addField(final JsonObjectBuilder builder, final String key, final Long value, boolean allowNullValues) {
        if (value != null) {
            builder.add(key, value);
        } else if (allowNullValues) {
            builder.add(key, JsonValue.NULL);
        }
    }

    protected void addField(final JsonObjectBuilder builder, final String key, final Integer value, boolean allowNullValues) {
        if (value != null) {
            builder.add(key, value);
        } else if (allowNullValues) {
            builder.add(key, JsonValue.NULL);
        }
    }

    protected void addField(final JsonObjectBuilder builder, final String key, final String value, boolean allowNullValues) {
        if (value != null) {
            builder.add(key, value);
        } else if (allowNullValues) {
            builder.add(key, JsonValue.NULL);
        }
    }

    private class JsonRecordReader implements RecordReader {

        private RecordSchema recordSchema;
        private final JsonParser jsonParser;
        private final boolean array;
        private final JsonNode firstJsonNode;
        private boolean firstObjectConsumed = false;

        public JsonRecordReader(final InputStream in, RecordSchema recordSchema) throws IOException, MalformedRecordException {
            this.recordSchema = recordSchema;
            try {
                jsonParser = new JsonFactory().createParser(in);
                jsonParser.setCodec(new ObjectMapper());
                JsonToken token = jsonParser.nextToken();
                if (token == JsonToken.START_ARRAY) {
                    array = true;
                    token = jsonParser.nextToken();
                } else {
                    array = false;
                }
                if (token == JsonToken.START_OBJECT) {
                    firstJsonNode = jsonParser.readValueAsTree();
                } else {
                    firstJsonNode = null;
                }
            } catch (final JsonParseException e) {
                throw new MalformedRecordException("Could not parse data as JSON", e);
            }
        }

        @Override
        public void close() throws IOException {
            jsonParser.close();
        }

        @Override
        public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException {
            if (firstObjectConsumed && !array) {
                return null;
            }

            JsonNode nextNode = getNextJsonNode();
            if (nextNode == null) {
                return null;
            }

            try {
                return convertJsonNodeToRecord(nextNode, getSchema(), null, coerceTypes, dropUnknownFields);
            } catch (final MalformedRecordException | IOException e) {
                throw e;
            } catch (final Exception e) {
                throw new MalformedRecordException("Failed to convert data into a Record object with the given schema", e);
            }
        }

        @Override
        public RecordSchema getSchema() throws MalformedRecordException {
            return recordSchema;
        }

        private JsonNode getNextJsonNode() throws IOException, MalformedRecordException {
            if (!firstObjectConsumed) {
                firstObjectConsumed = true;
                return firstJsonNode;
            }
            while (true) {
                final JsonToken token = jsonParser.nextToken();
                if (token == null) {
                    return null;
                }
                switch (token) {
                    case END_OBJECT:
                        continue;
                    case START_OBJECT:
                        return jsonParser.readValueAsTree();
                    case END_ARRAY:
                    case START_ARRAY:
                        return null;
                    default:
                        throw new MalformedRecordException("Expected to get a JSON Object but got a token of type " + token.name());
                }
            }
        }

        private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final String fieldNamePrefix,
                final boolean coerceTypes, final boolean dropUnknown) throws IOException, MalformedRecordException {

            final Map<String, Object> values = new HashMap<>(schema.getFieldCount() * 2);

            if (dropUnknown) {
                for (final RecordField recordField : schema.getFields()) {
                    final JsonNode childNode = getChildNode(jsonNode, recordField);
                    if (childNode == null) {
                        continue;
                    }

                    final String fieldName = recordField.getFieldName();
                    final Object value;

                    if (coerceTypes) {
                        final DataType desiredType = recordField.getDataType();
                        final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                        value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                    } else {
                        value = getRawNodeValue(childNode, recordField == null ? null : recordField.getDataType());
                    }

                    values.put(fieldName, value);
                }
            } else {
                final Iterator<String> fieldNames = jsonNode.fieldNames();
                while (fieldNames.hasNext()) {
                    final String fieldName = fieldNames.next();
                    final JsonNode childNode = jsonNode.get(fieldName);
                    final RecordField recordField = schema.getField(fieldName).orElse(null);
                    final Object value;

                    if (coerceTypes && recordField != null) {
                        final DataType desiredType = recordField.getDataType();
                        final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                        value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                    } else {
                        value = getRawNodeValue(childNode, recordField == null ? null : recordField.getDataType());
                    }

                    values.put(fieldName, value);
                }
            }

            final Supplier<String> supplier = () -> jsonNode.toString();
            return new MapRecord(schema, values, SerializedForm.of(supplier, "application/json"), false, dropUnknown);
        }

        private JsonNode getChildNode(final JsonNode jsonNode, final RecordField field) {
            if (jsonNode.has(field.getFieldName())) {
                return jsonNode.get(field.getFieldName());
            }
            for (final String alias : field.getAliases()) {
                if (jsonNode.has(alias)) {
                    return jsonNode.get(alias);
                }
            }
            return null;
        }

        protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType, final boolean dropUnknown) throws IOException, MalformedRecordException {
            if (fieldNode == null || fieldNode.isNull()) {
                return null;
            }

            switch (desiredType.getFieldType()) {
                case BOOLEAN:
                case BYTE:
                case CHAR:
                case DOUBLE:
                case FLOAT:
                case INT:
                case BIGINT:
                case DECIMAL:
                case LONG:
                case SHORT:
                case STRING:
                case DATE:
                case TIME:
                case TIMESTAMP: {
                    final Object rawValue = getRawNodeValue(fieldNode, null);
                    return DataTypeUtils.convertType(rawValue, desiredType, fieldName);
                }
                case MAP: {
                    final DataType valueType = ((MapDataType) desiredType).getValueType();

                    final Map<String, Object> map = new HashMap<>();
                    final Iterator<String> fieldNameItr = fieldNode.fieldNames();
                    while (fieldNameItr.hasNext()) {
                        final String childName = fieldNameItr.next();
                        final JsonNode childNode = fieldNode.get(childName);
                        final Object childValue = convertField(childNode, fieldName, valueType, dropUnknown);
                        map.put(childName, childValue);
                    }

                    return map;
                }
                case ARRAY: {
                    final ArrayNode arrayNode = (ArrayNode) fieldNode;
                    final int numElements = arrayNode.size();
                    final Object[] arrayElements = new Object[numElements];
                    int count = 0;
                    for (final JsonNode node : arrayNode) {
                        final DataType elementType = ((ArrayDataType) desiredType).getElementType();
                        final Object converted = convertField(node, fieldName, elementType, dropUnknown);
                        arrayElements[count++] = converted;
                    }

                    return arrayElements;
                }
                case RECORD: {
                    if (fieldNode.isObject()) {
                        RecordSchema childSchema;
                        if (desiredType instanceof RecordDataType) {
                            childSchema = ((RecordDataType) desiredType).getChildSchema();
                        } else {
                            return null;
                        }

                        if (childSchema == null) {
                            final List<RecordField> fields = new ArrayList<>();
                            final Iterator<String> fieldNameItr = fieldNode.fieldNames();
                            while (fieldNameItr.hasNext()) {
                                fields.add(new RecordField(fieldNameItr.next(), RecordFieldType.STRING.getDataType()));
                            }

                            childSchema = new SimpleRecordSchema(fields);
                        }

                        return convertJsonNodeToRecord(fieldNode, childSchema, fieldName + ".", true, dropUnknown);
                    } else {
                        return null;
                    }
                }
                case CHOICE: {
                    return DataTypeUtils.convertType(getRawNodeValue(fieldNode, null), desiredType, fieldName);
                }
            }

            return null;
        }

        protected Object getRawNodeValue(final JsonNode fieldNode, final DataType dataType) throws IOException {
            if (fieldNode == null || fieldNode.isNull()) {
                return null;
            }

            if (fieldNode.isNumber()) {
                return fieldNode.numberValue();
            }

            if (fieldNode.isBinary()) {
                return fieldNode.binaryValue();
            }

            if (fieldNode.isBoolean()) {
                return fieldNode.booleanValue();
            }

            if (fieldNode.isTextual()) {
                return fieldNode.textValue();
            }

            if (fieldNode.isArray()) {
                final ArrayNode arrayNode = (ArrayNode) fieldNode;
                final int numElements = arrayNode.size();
                final Object[] arrayElements = new Object[numElements];
                int count = 0;

                final DataType elementDataType;
                if (dataType != null && dataType.getFieldType() == RecordFieldType.ARRAY) {
                    final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                    elementDataType = arrayDataType.getElementType();
                } else {
                    elementDataType = null;
                }

                for (final JsonNode node : arrayNode) {
                    final Object value = getRawNodeValue(node, elementDataType);
                    arrayElements[count++] = value;
                }

                return arrayElements;
            }

            if (fieldNode.isObject()) {
                RecordSchema childSchema;
                if (dataType != null && RecordFieldType.RECORD == dataType.getFieldType()) {
                    final RecordDataType recordDataType = (RecordDataType) dataType;
                    childSchema = recordDataType.getChildSchema();
                } else {
                    childSchema = null;
                }

                if (childSchema == null) {
                    childSchema = new SimpleRecordSchema(Collections.emptyList());
                }

                final Iterator<String> fieldNames = fieldNode.fieldNames();
                final Map<String, Object> childValues = new HashMap<>();
                while (fieldNames.hasNext()) {
                    final String childFieldName = fieldNames.next();
                    final Object childValue = getRawNodeValue(fieldNode.get(childFieldName), dataType);
                    childValues.put(childFieldName, childValue);
                }

                final MapRecord record = new MapRecord(childSchema, childValues);
                return record;
            }

            return null;
        }

    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();

        try (final SiteToSiteClient client = SiteToSiteUtils.getClient(context, verificationLogger, null)) {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);

            // If transaction is null, indicates that all nodes are penalized
            if (transaction == null) {
                verificationResults.add(new ConfigVerificationResult.Builder()
                    .verificationStepName(ESTABLISH_COMMUNICATION)
                    .outcome(Outcome.SKIPPED)
                    .explanation("All nodes in destination NiFi are currently 'penalized', meaning that there have been recent failures communicating with the destination NiFi, or that" +
                        " the NiFi instance is applying backpressure")
                    .build());
            } else {
                transaction.cancel("Just verifying configuration");

                verificationResults.add(new ConfigVerificationResult.Builder()
                    .verificationStepName(ESTABLISH_COMMUNICATION)
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Established connection to destination NiFi instance and Received indication that it is ready to ready to receive data")
                    .build());
            }
        } catch (final Exception e) {
            verificationLogger.error("Failed to establish site-to-site connection", e);

            verificationResults.add(new ConfigVerificationResult.Builder()
                .verificationStepName(ESTABLISH_COMMUNICATION)
                .outcome(Outcome.FAILED)
                .explanation("Failed to establish Site-to-Site Connection: " + e)
                .build());
        }

        return verificationResults;
    }
}
