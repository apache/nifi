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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public abstract class AbstractJsonRowRecordReader implements RecordReader {
    private final ComponentLog logger;
    private final JsonParser jsonParser;
    private final JsonNode firstJsonNode;
    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    private boolean firstObjectConsumed = false;

    private static final JsonFactory jsonFactory = new JsonFactory();
    private static final ObjectMapper codec = new ObjectMapper();


    public AbstractJsonRowRecordReader(final InputStream in, final ComponentLog logger, final String dateFormat, final String timeFormat, final String timestampFormat)
            throws IOException, MalformedRecordException {

        this.logger = logger;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        try {
            jsonParser = jsonFactory.createJsonParser(in);
            jsonParser.setCodec(codec);

            JsonToken token = jsonParser.nextToken();
            if (token == JsonToken.START_ARRAY) {
                token = jsonParser.nextToken(); // advance to START_OBJECT token
            }

            if (token == JsonToken.START_OBJECT) { // could be END_ARRAY also
                firstJsonNode = jsonParser.readValueAsTree();
            } else {
                firstJsonNode = null;
            }
        } catch (final JsonParseException e) {
            throw new MalformedRecordException("Could not parse data as JSON", e);
        }
    }

    protected Supplier<DateFormat> getLazyDateFormat() {
        return LAZY_DATE_FORMAT;
    }

    protected Supplier<DateFormat> getLazyTimeFormat() {
        return LAZY_TIME_FORMAT;
    }

    protected Supplier<DateFormat> getLazyTimestampFormat() {
        return LAZY_TIMESTAMP_FORMAT;
    }


    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        final JsonNode nextNode = getNextJsonNode();
        if (nextNode == null) {
            return null;
        }

        final RecordSchema schema = getSchema();
        try {
            return convertJsonNodeToRecord(nextNode, schema, coerceTypes, dropUnknownFields);
        } catch (final MalformedRecordException mre) {
            throw mre;
        } catch (final Exception e) {
            logger.debug("Failed to convert JSON Element {} into a Record object using schema {} due to {}", new Object[] {nextNode, schema, e.toString(), e});
            throw new MalformedRecordException("Successfully parsed a JSON object from input but failed to convert into a Record object with the given schema", e);
        }
    }

    protected Object getRawNodeValue(final JsonNode fieldNode, final String fieldName) throws IOException {
        return getRawNodeValue(fieldNode, null, fieldName);
    }

    protected Object getRawNodeValue(final JsonNode fieldNode, final DataType dataType, final String fieldName) throws IOException {
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }

        if (fieldNode.isNumber()) {
            return fieldNode.getNumberValue();
        }

        if (fieldNode.isBinary()) {
            return fieldNode.getBinaryValue();
        }

        if (fieldNode.isBoolean()) {
            return fieldNode.getBooleanValue();
        }

        if (fieldNode.isTextual()) {
            final String textValue = fieldNode.getTextValue();
            if (dataType == null) {
                return textValue;
            }

            switch (dataType.getFieldType()) {
                case DATE:
                case TIME:
                case TIMESTAMP:
                    try {
                        return DataTypeUtils.convertType(textValue, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                    } catch (final Exception e) {
                        return textValue;
                    }
                default:
                    return textValue;
            }
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
            } else if (dataType != null && dataType.getFieldType() == RecordFieldType.CHOICE) {
                List<DataType> possibleSubTypes = ((ChoiceDataType)dataType).getPossibleSubTypes();

                for (DataType possibleSubType : possibleSubTypes) {
                    if (possibleSubType.getFieldType() == RecordFieldType.ARRAY) {
                        ArrayDataType possibleArrayDataType = (ArrayDataType)possibleSubType;
                        DataType possibleElementType = possibleArrayDataType.getElementType();

                        final Object[] possibleArrayElements = new Object[numElements];
                        int elementCounter = 0;
                        for (final JsonNode node : arrayNode) {
                            final Object value = getRawNodeValue(node, possibleElementType, fieldName);
                            possibleArrayElements[elementCounter++] = value;
                        }

                        if (DataTypeUtils.isArrayTypeCompatible(possibleArrayElements, possibleElementType, true)) {
                            return possibleArrayElements;
                        }
                    }
                }

                logger.debug("Couldn't find proper schema for '{}'. This could lead to some fields filtered out.", fieldName);

                elementDataType = dataType;
            } else {
                elementDataType = dataType;
            }

            for (final JsonNode node : arrayNode) {
                final Object value = getRawNodeValue(node, elementDataType, fieldName);
                arrayElements[count++] = value;
            }

            return arrayElements;
        }

        if (fieldNode.isObject()) {
            RecordSchema childSchema = null;
            if (dataType != null && RecordFieldType.MAP == dataType.getFieldType()) {
                return getMapFromRawValue(fieldNode, dataType, fieldName);
            }

            return getRecordFromRawValue(fieldNode, dataType);
        }

        return null;
    }

    private Map<String, Object> getMapFromRawValue(final JsonNode fieldNode, final DataType dataType, final String fieldName) throws IOException {
        if (dataType == null || dataType.getFieldType() != RecordFieldType.MAP) {
            return null;
        }

        final MapDataType mapDataType = (MapDataType) dataType;
        final DataType valueType = mapDataType.getValueType();

        final Map<String, Object> mapValue = new HashMap<>();

        final Iterator<Map.Entry<String, JsonNode>> fieldItr = fieldNode.getFields();
        while (fieldItr.hasNext()) {
            final Map.Entry<String, JsonNode> entry = fieldItr.next();
            final String elementName = entry.getKey();
            final JsonNode elementNode = entry.getValue();

            final Object nodeValue = getRawNodeValue(elementNode, valueType, fieldName + "['" + elementName + "']");
            mapValue.put(elementName, nodeValue);
        }

        return mapValue;
    }

    private Record getRecordFromRawValue(final JsonNode fieldNode, final DataType dataType) throws IOException {
        RecordSchema childSchema = null;
        if (dataType != null && RecordFieldType.RECORD == dataType.getFieldType()) {
            final RecordDataType recordDataType = (RecordDataType) dataType;
            childSchema = recordDataType.getChildSchema();
        } else if (dataType != null && RecordFieldType.CHOICE == dataType.getFieldType()) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;

            List<DataType> possibleSubTypes = choiceDataType.getPossibleSubTypes();

            for (final DataType possibleDataType : possibleSubTypes) {
                final Record record = createOptionalRecord(fieldNode, possibleDataType, true);
                if (record != null) {
                    return record;
                }
            }

            for (final DataType possibleDataType : possibleSubTypes) {
                final Record record = createOptionalRecord(fieldNode, possibleDataType, false);
                if (record != null) {
                    return record;
                }
            }
        }

        if (childSchema == null) {
            childSchema = new SimpleRecordSchema(Collections.emptyList());
        }

        return createRecordFromRawValue(fieldNode, childSchema);
    }

    private Record createOptionalRecord(final JsonNode fieldNode, final DataType dataType, final boolean strict) throws IOException {
        if (dataType.getFieldType() == RecordFieldType.RECORD) {
            final RecordSchema possibleSchema = ((RecordDataType) dataType).getChildSchema();
            final Record possibleRecord = createRecordFromRawValue(fieldNode, possibleSchema);

            if (DataTypeUtils.isCompatibleDataType(possibleRecord, dataType, strict)) {
                return possibleRecord;
            }
        } else if (dataType.getFieldType() == RecordFieldType.ARRAY) {
            final ArrayDataType arrayDataType = (ArrayDataType) dataType;
            final DataType elementType = arrayDataType.getElementType();
            final Record record = createOptionalRecord(fieldNode, elementType, strict);
            return record;
        }

        return null;
    }

    private Record createRecordFromRawValue(final JsonNode fieldNode, final RecordSchema childSchema) throws IOException {
        final Iterator<String> fieldNames = fieldNode.getFieldNames();
        final Map<String, Object> childValues = new HashMap<>();
        while (fieldNames.hasNext()) {
            final String childFieldName = fieldNames.next();

            final DataType childDataType = childSchema.getDataType(childFieldName).orElse(null);
            final Object childValue = getRawNodeValue(fieldNode.get(childFieldName), childDataType, childFieldName);
            childValues.put(childFieldName, childValue);
        }

        final MapRecord record = new MapRecord(childSchema, childValues);
        return record;
    }

    protected JsonNode getNextJsonNode() throws IOException, MalformedRecordException {
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
                    continue;

                default:
                    throw new MalformedRecordException("Expected to get a JSON Object but got a token of type " + token.name());
            }
        }
    }

    @Override
    public void close() throws IOException {
        jsonParser.close();
    }

    protected abstract Record convertJsonNodeToRecord(JsonNode nextNode, RecordSchema schema, boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException;
}
