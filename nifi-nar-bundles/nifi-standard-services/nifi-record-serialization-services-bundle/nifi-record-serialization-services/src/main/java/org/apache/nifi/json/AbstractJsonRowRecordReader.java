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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

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
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;


public abstract class AbstractJsonRowRecordReader implements RecordReader {
    private final ComponentLog logger;
    private final JsonParser jsonParser;
    private final boolean array;
    private final JsonNode firstJsonNode;

    private boolean firstObjectConsumed = false;

    private static final JsonFactory jsonFactory = new JsonFactory();
    private static final ObjectMapper codec = new ObjectMapper();

    public AbstractJsonRowRecordReader(final InputStream in, final ComponentLog logger) throws IOException, MalformedRecordException {
        this.logger = logger;

        try {
            jsonParser = jsonFactory.createJsonParser(in);
            jsonParser.setCodec(codec);

            JsonToken token = jsonParser.nextToken();
            if (token == JsonToken.START_ARRAY) {
                array = true;
                token = jsonParser.nextToken(); // advance to START_OBJECT token
            } else {
                array = false;
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


    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (firstObjectConsumed && !array) {
            return null;
        }

        final JsonNode nextNode = getNextJsonNode();
        final RecordSchema schema = getSchema();
        try {
            return convertJsonNodeToRecord(nextNode, schema, coerceTypes, dropUnknownFields);
        } catch (final MalformedRecordException mre) {
            throw mre;
        } catch (final IOException ioe) {
            throw ioe;
        } catch (final Exception e) {
            logger.debug("Failed to convert JSON Element {} into a Record object using schema {} due to {}", new Object[] {nextNode, schema, e.toString(), e});
            throw new MalformedRecordException("Successfully parsed a JSON object from input but failed to convert into a Record object with the given schema", e);
        }
    }

    protected Object getRawNodeValue(final JsonNode fieldNode) throws IOException {
        return getRawNodeValue(fieldNode, null);
    }

    protected Object getRawNodeValue(final JsonNode fieldNode, final DataType dataType) throws IOException {
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
            return fieldNode.getTextValue();
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

            final Iterator<String> fieldNames = fieldNode.getFieldNames();
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


    private JsonNode getNextJsonNode() throws JsonParseException, IOException, MalformedRecordException {
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


    @Override
    public void close() throws IOException {
        jsonParser.close();
    }

    protected JsonParser getJsonParser() {
        return jsonParser;
    }

    protected JsonFactory getJsonFactory() {
        return jsonFactory;
    }

    protected Optional<JsonNode> getFirstJsonNode() {
        return Optional.ofNullable(firstJsonNode);
    }

    protected abstract Record convertJsonNodeToRecord(JsonNode nextNode, RecordSchema schema, boolean coerceTypes, boolean dropUnknownFields) throws IOException, MalformedRecordException;
}
