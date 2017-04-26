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

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;


public class JsonTreeRowRecordReader extends AbstractJsonRowRecordReader {
    private final RecordSchema schema;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    public JsonTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema,
        final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException, MalformedRecordException {
        super(in, logger);
        this.schema = schema;

        this.dateFormat = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        this.timeFormat = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        this.timestampFormat = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);
    }


    @Override
    protected Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema) throws IOException, MalformedRecordException {
        return convertJsonNodeToRecord(jsonNode, schema, null);
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final String fieldNamePrefix) throws IOException, MalformedRecordException {
        if (jsonNode == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());
        for (final RecordField field : schema.getFields()) {
            final String fieldName = field.getFieldName();

            final JsonNode fieldNode = getJsonNode(jsonNode, field);

            final DataType desiredType = field.getDataType();
            final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
            final Object value = convertField(fieldNode, fullFieldName, desiredType);
            values.put(fieldName, value);
        }

        final Supplier<String> supplier = () -> jsonNode.toString();
        return new MapRecord(schema, values, SerializedForm.of(supplier, "application/json"));
    }

    private JsonNode getJsonNode(final JsonNode parent, final RecordField field) {
        JsonNode fieldNode = parent.get(field.getFieldName());
        if (fieldNode != null) {
            return fieldNode;
        }

        for (final String alias : field.getAliases()) {
            fieldNode = parent.get(alias);
            if (fieldNode != null) {
                return fieldNode;
            }
        }

        return fieldNode;
    }

    protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType) throws IOException, MalformedRecordException {
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }

        switch (desiredType.getFieldType()) {
            case BOOLEAN:
                return DataTypeUtils.toBoolean(getRawNodeValue(fieldNode), fieldName);
            case BYTE:
                return DataTypeUtils.toByte(getRawNodeValue(fieldNode), fieldName);
            case CHAR:
                return DataTypeUtils.toCharacter(getRawNodeValue(fieldNode), fieldName);
            case DOUBLE:
                return DataTypeUtils.toDouble(getRawNodeValue(fieldNode), fieldName);
            case FLOAT:
                return DataTypeUtils.toFloat(getRawNodeValue(fieldNode), fieldName);
            case INT:
                return DataTypeUtils.toInteger(getRawNodeValue(fieldNode), fieldName);
            case LONG:
                return DataTypeUtils.toLong(getRawNodeValue(fieldNode), fieldName);
            case SHORT:
                return DataTypeUtils.toShort(getRawNodeValue(fieldNode), fieldName);
            case STRING:
                return DataTypeUtils.toString(getRawNodeValue(fieldNode),
                    () -> DataTypeUtils.getDateFormat(desiredType.getFieldType(), () -> dateFormat, () -> timeFormat, () -> timestampFormat));
            case DATE:
                return DataTypeUtils.toDate(getRawNodeValue(fieldNode), () -> dateFormat, fieldName);
            case TIME:
                return DataTypeUtils.toTime(getRawNodeValue(fieldNode), () -> timeFormat, fieldName);
            case TIMESTAMP:
                return DataTypeUtils.toTimestamp(getRawNodeValue(fieldNode), () -> timestampFormat, fieldName);
            case MAP: {
                final DataType valueType = ((MapDataType) desiredType).getValueType();

                final Map<String, Object> map = new HashMap<>();
                final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
                while (fieldNameItr.hasNext()) {
                    final String childName = fieldNameItr.next();
                    final JsonNode childNode = fieldNode.get(childName);
                    final Object childValue = convertField(childNode, fieldName + "." + childName, valueType);
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
                    final Object converted = convertField(node, fieldName, elementType);
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
                        final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
                        while (fieldNameItr.hasNext()) {
                            fields.add(new RecordField(fieldNameItr.next(), RecordFieldType.STRING.getDataType()));
                        }

                        childSchema = new SimpleRecordSchema(fields);
                    }

                    return convertJsonNodeToRecord(fieldNode, childSchema, fieldName + ".");
                } else {
                    return null;
                }
            }
        }

        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
