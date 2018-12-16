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

package org.apache.nifi.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.jsfr.json.JacksonParser;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.compiler.JsonPathCompiler;
import org.jsfr.json.provider.JacksonProvider;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class StreamingJsonRowRecordReader implements RecordReader {
    private final ComponentLog logger;
    private final InputStream in;
    private RecordSchema schema;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    private JsonSurfer surfer;
    private Iterator iterator;

    public StreamingJsonRowRecordReader(final String jsonPath, final RecordSchema schema, final InputStream in, final ComponentLog logger,
                                   final String dateFormat, final String timeFormat, final String timestampFormat) {

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        this.schema = schema;
        this.in = in;
        this.logger = logger;

        this.surfer = new JsonSurfer(JacksonParser.INSTANCE, JacksonProvider.INSTANCE);
        this.iterator = surfer.iterator(in, JsonPathCompiler.compile(jsonPath));
    }

    @Override
    public Record nextRecord() {
        return nextRecord(true, true);
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) {
        if (!iterator.hasNext()) {
            return null;
        }

        JsonNode node = (JsonNode) iterator.next();

        if (!node.isObject()) {
            throw new ProcessException("The reader is not operating on an array of objects.");
        }

        try {
            return convertJsonNodeToRecord(node, schema, coerceTypes, dropUnknownFields);
        } catch (Exception e) {
            logger.error("Error", e);
            throw new ProcessException(e);
        }
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema,
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
                    final String fullFieldName = fieldName;
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
                    final String fullFieldName = fieldName;
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
            case LONG:
            case SHORT:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                final Object rawValue = getRawNodeValue(fieldNode);
                final Object converted = DataTypeUtils.convertType(rawValue, desiredType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                return converted;
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

                    return convertJsonNodeToRecord(fieldNode, childSchema, true, dropUnknown);
                } else {
                    return null;
                }
            }
            case CHOICE: {
                return DataTypeUtils.convertType(getRawNodeValue(fieldNode, desiredType), desiredType, fieldName);
            }
        }

        return null;
    }


    private Record convert(JsonNode jsonNode, RecordSchema schema, final boolean coerceTypes, final boolean dropUnknownFields) {
        if (!jsonNode.isObject()) {
            throw new ProcessException("Node was not an object.");
        }

        Record retVal = null;
        Map<String, Object> current = new HashMap<>();
        ObjectNode on = (ObjectNode)jsonNode;
        List<String> schemaFieldNames = schema.getFieldNames();
        Iterator<Map.Entry<String, JsonNode>> fields = on.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> _temp = fields.next();
            if (dropUnknownFields && !schemaFieldNames.contains(_temp.getKey())) {
                continue;
            }


        }

        return retVal;
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

    protected Object getRawNodeValue(final JsonNode fieldNode) throws IOException {
        return getRawNodeValue(fieldNode, null);
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
                elementDataType = dataType;
            }

            for (final JsonNode node : arrayNode) {
                final Object value = getRawNodeValue(node, elementDataType);
                arrayElements[count++] = value;
            }

            return arrayElements;
        }

        if (fieldNode.isObject()) {
            RecordSchema childSchema = null;
            if (dataType != null && RecordFieldType.RECORD == dataType.getFieldType()) {
                final RecordDataType recordDataType = (RecordDataType) dataType;
                childSchema = recordDataType.getChildSchema();
            } else if (dataType != null && RecordFieldType.CHOICE == dataType.getFieldType()) {
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;

                for (final DataType possibleDataType : choiceDataType.getPossibleSubTypes()) {
                    if (possibleDataType.getFieldType() != RecordFieldType.RECORD) {
                        continue;
                    }

                    final RecordSchema possibleSchema = ((RecordDataType) possibleDataType).getChildSchema();

                    final Map<String, Object> childValues = new HashMap<>();
                    final Iterator<String> fieldNames = fieldNode.fieldNames();
                    while (fieldNames.hasNext()) {
                        final String childFieldName = fieldNames.next();

                        final Object childValue = getRawNodeValue(fieldNode.get(childFieldName), possibleSchema.getDataType(childFieldName).orElse(null));
                        childValues.put(childFieldName, childValue);
                    }

                    final Record possibleRecord = new MapRecord(possibleSchema, childValues);
                    if (DataTypeUtils.isCompatibleDataType(possibleRecord, possibleDataType)) {
                        return possibleRecord;
                    }
                }
            }

            if (childSchema == null) {
                childSchema = new SimpleRecordSchema(Collections.emptyList());
            }

            final Iterator<String> fieldNames = fieldNode.fieldNames();
            final Map<String, Object> childValues = new HashMap<>();
            while (fieldNames.hasNext()) {
                final String childFieldName = fieldNames.next();

                final DataType childDataType = childSchema.getDataType(childFieldName).orElse(null);
                final Object childValue = getRawNodeValue(fieldNode.get(childFieldName), childDataType);
                childValues.put(childFieldName, childValue);
            }

            final MapRecord record = new MapRecord(childSchema, childValues);
            return record;
        }

        return null;
    }


    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() throws IOException {

    }
}
