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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.schema.inference.RecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class JsonRecordSource implements RecordSource<JsonNode> {
    private static final Logger logger = LoggerFactory.getLogger(JsonRecordSource.class);
    private static final JsonFactory jsonFactory;
    private final JsonParser jsonParser;
    private final StartingFieldStrategy strategy;
    private final String startingFieldName;

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.setCodec(new ObjectMapper());
    }

    public JsonRecordSource(final InputStream in) throws IOException {
        jsonParser = jsonFactory.createParser(in);
        strategy = null;
        startingFieldName = null;
    }

    public JsonRecordSource(final InputStream in, final StartingFieldStrategy strategy, final String startingFieldName) throws IOException {
        jsonParser = jsonFactory.createParser(in);
        this.strategy = strategy;
        this.startingFieldName = startingFieldName;

        if (strategy == StartingFieldStrategy.NESTED_FIELD) {
            final SerializedString serializedNestedField = new SerializedString(this.startingFieldName);
            while (!jsonParser.nextFieldName(serializedNestedField) && jsonParser.hasCurrentToken());
            logger.debug("Parsing starting at nested field [{}]", startingFieldName);
        }
    }

    @Override
    public JsonNode next() throws IOException {
        while (true) {
            final JsonToken token = jsonParser.nextToken();
            if (token == null) {
                return null;
            }

            if (token == JsonToken.START_OBJECT) {
                return jsonParser.readValueAsTree();
            }

            if (strategy == StartingFieldStrategy.NESTED_FIELD && (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT)) {
                return null;
            }
        }
    }
}
