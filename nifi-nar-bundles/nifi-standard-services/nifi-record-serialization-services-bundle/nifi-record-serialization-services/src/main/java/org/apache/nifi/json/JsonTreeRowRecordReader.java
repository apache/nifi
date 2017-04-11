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
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;


public class JsonTreeRowRecordReader extends AbstractJsonRowRecordReader {
    private final RecordSchema schema;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;

    public JsonTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema,
        final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException, MalformedRecordException {
        super(in, logger);
        this.schema = schema;

        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
    }


    @Override
    protected Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema) throws IOException, MalformedRecordException {
        if (jsonNode == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());
        for (int i = 0; i < schema.getFieldCount(); i++) {
            final RecordField field = schema.getField(i);
            final String fieldName = field.getFieldName();
            final JsonNode fieldNode = jsonNode.get(fieldName);

            final DataType desiredType = field.getDataType();
            final Object value = convertField(fieldNode, fieldName, desiredType);
            values.put(fieldName, value);
        }

        return new MapRecord(schema, values);
    }

    protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType) throws IOException, MalformedRecordException {
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }

        switch (desiredType.getFieldType()) {
            case BOOLEAN:
                return DataTypeUtils.toBoolean(getRawNodeValue(fieldNode));
            case BYTE:
                return DataTypeUtils.toByte(getRawNodeValue(fieldNode));
            case CHAR:
                return DataTypeUtils.toCharacter(getRawNodeValue(fieldNode));
            case DOUBLE:
                return DataTypeUtils.toDouble(getRawNodeValue(fieldNode));
            case FLOAT:
                return DataTypeUtils.toFloat(getRawNodeValue(fieldNode));
            case INT:
                return DataTypeUtils.toInteger(getRawNodeValue(fieldNode));
            case LONG:
                return DataTypeUtils.toLong(getRawNodeValue(fieldNode));
            case SHORT:
                return DataTypeUtils.toShort(getRawNodeValue(fieldNode));
            case STRING:
                return DataTypeUtils.toString(getRawNodeValue(fieldNode), dateFormat, timeFormat, timestampFormat);
            case DATE:
                return DataTypeUtils.toDate(getRawNodeValue(fieldNode), dateFormat);
            case TIME:
                return DataTypeUtils.toTime(getRawNodeValue(fieldNode), timeFormat);
            case TIMESTAMP:
                return DataTypeUtils.toTimestamp(getRawNodeValue(fieldNode), timestampFormat);
            case ARRAY: {
                final ArrayNode arrayNode = (ArrayNode) fieldNode;
                final int numElements = arrayNode.size();
                final Object[] arrayElements = new Object[numElements];
                int count = 0;
                for (final JsonNode node : arrayNode) {
                    final DataType elementType;
                    if (desiredType instanceof ArrayDataType) {
                        elementType = ((ArrayDataType) desiredType).getElementType();
                    } else {
                        elementType = determineFieldType(node);
                    }

                    final Object converted = convertField(node, fieldName, elementType);
                    arrayElements[count++] = converted;
                }

                return arrayElements;
            }
            case RECORD: {
                if (fieldNode.isObject()) {
                    final RecordSchema childSchema;
                    if (desiredType instanceof RecordDataType) {
                        childSchema = ((RecordDataType) desiredType).getChildSchema();
                    } else {
                        return null;
                    }

                    return convertJsonNodeToRecord(fieldNode, childSchema);
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
