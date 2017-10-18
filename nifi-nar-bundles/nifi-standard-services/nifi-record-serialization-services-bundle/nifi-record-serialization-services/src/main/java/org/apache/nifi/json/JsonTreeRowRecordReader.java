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
import java.util.LinkedHashMap;
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

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;


    public JsonTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema,
        final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException, MalformedRecordException {
        super(in, logger);
        this.schema = schema;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;
    }


    @Override
    protected Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final boolean coerceTypes, final boolean dropUnknownFields)
        throws IOException, MalformedRecordException {
        return convertJsonNodeToRecord(jsonNode, schema, coerceTypes, dropUnknownFields, null);
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final boolean coerceTypes, final boolean dropUnknown, final String fieldNamePrefix)
        throws IOException, MalformedRecordException {
        if (jsonNode == null) {
            return null;
        }

        return convertJsonNodeToRecord(jsonNode, schema, fieldNamePrefix, coerceTypes, dropUnknown);
    }


    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final String fieldNamePrefix,
            final boolean coerceTypes, final boolean dropUnknown) throws IOException, MalformedRecordException {

        final Map<String, Object> values = new LinkedHashMap<>();
        final Iterator<String> fieldNames = jsonNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();
            final JsonNode childNode = jsonNode.get(fieldName);

            final RecordField recordField = schema.getField(fieldName).orElse(null);
            if (recordField == null && dropUnknown) {
                continue;
            }

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
                final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
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
                        final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
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
                return DataTypeUtils.convertType(getRawNodeValue(fieldNode), desiredType, fieldName);
            }
        }

        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
