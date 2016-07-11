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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
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
    private final JsonFactory jsonFactory;
    private final boolean array;
    private final JsonNode firstJsonNode;

    private boolean firstObjectConsumed = false;

    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");


    public AbstractJsonRowRecordReader(final InputStream in, final ComponentLog logger) throws IOException, MalformedRecordException {
        this.logger = logger;

        jsonFactory = new JsonFactory();
        try {
            jsonParser = jsonFactory.createJsonParser(in);
            jsonParser.setCodec(new ObjectMapper());

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
    public Record nextRecord() throws IOException, MalformedRecordException {
        if (firstObjectConsumed && !array) {
            return null;
        }

        final JsonNode nextNode = getNextJsonNode();
        final RecordSchema schema = getSchema();
        try {
            return convertJsonNodeToRecord(nextNode, schema);
        } catch (final MalformedRecordException mre) {
            throw mre;
        } catch (final IOException ioe) {
            throw ioe;
        } catch (final Exception e) {
            logger.debug("Failed to convert JSON Element {} into a Record object using schema {} due to {}", new Object[] {nextNode, schema, e.toString(), e});
            throw new MalformedRecordException("Successfully parsed a JSON object from input but failed to convert into a Record object with the given schema", e);
        }
    }

    protected DataType determineFieldType(final JsonNode node) {
        if (node.isDouble()) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (node.isBoolean()) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        if (node.isFloatingPointNumber()) {
            return RecordFieldType.FLOAT.getDataType();
        }
        if (node.isBigInteger()) {
            return RecordFieldType.BIGINT.getDataType();
        }
        if (node.isBigDecimal()) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (node.isLong()) {
            return RecordFieldType.LONG.getDataType();
        }
        if (node.isInt()) {
            return RecordFieldType.INT.getDataType();
        }
        if (node.isTextual()) {
            return RecordFieldType.STRING.getDataType();
        }
        if (node.isArray()) {
            return RecordFieldType.ARRAY.getDataType();
        }

        final RecordSchema childSchema = determineSchema(node);
        return RecordFieldType.RECORD.getDataType(childSchema);
    }

    protected RecordSchema determineSchema(final JsonNode jsonNode) {
        final List<RecordField> recordFields = new ArrayList<>();

        final Iterator<Map.Entry<String, JsonNode>> itr = jsonNode.getFields();
        while (itr.hasNext()) {
            final Map.Entry<String, JsonNode> entry = itr.next();
            final String elementName = entry.getKey();
            final JsonNode node = entry.getValue();

            DataType dataType = determineFieldType(node);
            recordFields.add(new RecordField(elementName, dataType));
        }

        return new SimpleRecordSchema(recordFields);
    }

    protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType) throws IOException, MalformedRecordException {
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }

        switch (desiredType.getFieldType()) {
            case BOOLEAN:
                return fieldNode.asBoolean();
            case BYTE:
                return (byte) fieldNode.asInt();
            case CHAR:
                final String text = fieldNode.asText();
                if (text.isEmpty()) {
                    return null;
                }
                return text.charAt(0);
            case DOUBLE:
                return fieldNode.asDouble();
            case FLOAT:
                return (float) fieldNode.asDouble();
            case INT:
                return fieldNode.asInt();
            case LONG:
                return fieldNode.asLong();
            case SHORT:
                return (short) fieldNode.asInt();
            case STRING:
                return fieldNode.asText();
            case DATE: {
                final String string = fieldNode.asText();
                if (string.isEmpty()) {
                    return null;
                }

                try {
                    final DateFormat dateFormat = new SimpleDateFormat(desiredType.getFormat());
                    dateFormat.setTimeZone(gmt);
                    final Date date = dateFormat.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    logger.warn("Failed to convert JSON field to Date for field {} (value {})", new Object[] {fieldName, string, e});
                    return null;
                }
            }
            case TIME: {
                final String string = fieldNode.asText();
                if (string.isEmpty()) {
                    return null;
                }

                try {
                    final DateFormat dateFormat = new SimpleDateFormat(desiredType.getFormat());
                    dateFormat.setTimeZone(gmt);
                    final Date date = dateFormat.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    logger.warn("Failed to convert JSON field to Time for field {} (value {})", new Object[] {fieldName, string, e});
                    return null;
                }
            }
            case TIMESTAMP: {
                final String string = fieldNode.asText();
                if (string.isEmpty()) {
                    return null;
                }

                try {
                    final DateFormat dateFormat = new SimpleDateFormat(desiredType.getFormat());
                    dateFormat.setTimeZone(gmt);
                    final Date date = dateFormat.parse(string);
                    return new java.sql.Date(date.getTime());
                } catch (ParseException e) {
                    logger.warn("Failed to convert JSON field to Timestamp for field {} (value {})", new Object[] {fieldName, string, e});
                    return null;
                }
            }
            case ARRAY: {
                final ArrayNode arrayNode = (ArrayNode) fieldNode;
                final int numElements = arrayNode.size();
                final Object[] arrayElements = new Object[numElements];
                int count = 0;
                for (final JsonNode node : arrayNode) {
                    final Object converted = convertField(node, fieldName, determineFieldType(node));
                    arrayElements[count++] = converted;
                }

                return arrayElements;
            }
            case RECORD: {
                if (fieldNode.isObject()) {
                    final Optional<RecordSchema> childSchema = desiredType.getChildRecordSchema();
                    if (!childSchema.isPresent()) {
                        return null;
                    }

                    return convertJsonNodeToRecord(fieldNode, childSchema.get());
                } else {
                    return fieldNode.toString();
                }
            }
        }

        return fieldNode.toString();
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

    protected abstract Record convertJsonNodeToRecord(final JsonNode nextNode, final RecordSchema schema) throws IOException, MalformedRecordException;
}
