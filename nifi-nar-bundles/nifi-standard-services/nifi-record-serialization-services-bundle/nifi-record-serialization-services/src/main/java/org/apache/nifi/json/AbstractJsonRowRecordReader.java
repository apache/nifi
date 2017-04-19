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
import java.util.Optional;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;


public abstract class AbstractJsonRowRecordReader implements RecordReader {
    private final ComponentLog logger;
    private final JsonParser jsonParser;
    private final JsonFactory jsonFactory;
    private final boolean array;
    private final JsonNode firstJsonNode;

    private boolean firstObjectConsumed = false;


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

    protected Object getRawNodeValue(final JsonNode fieldNode) throws IOException {
        if (fieldNode == null || !fieldNode.isValueNode()) {
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

    protected abstract Record convertJsonNodeToRecord(final JsonNode nextNode, final RecordSchema schema) throws IOException, MalformedRecordException;
}
