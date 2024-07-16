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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.schema.inference.RecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class JsonRecordSource implements RecordSource<JsonNode> {
    private static final Logger logger = LoggerFactory.getLogger(JsonRecordSource.class);

    private static final StreamReadConstraints DEFAULT_STREAM_READ_CONSTRAINTS = StreamReadConstraints.defaults();

    private static final boolean ALLOW_COMMENTS_ENABLED = true;

    private final JsonParser jsonParser;
    private final StartingFieldStrategy strategy;

    public JsonRecordSource(final InputStream in) throws IOException {
        this(in, null, null, DEFAULT_STREAM_READ_CONSTRAINTS);
    }

    public JsonRecordSource(final InputStream in, StreamReadConstraints streamReadConstraints) throws IOException {
        this(in, null, null, streamReadConstraints);
    }

    public JsonRecordSource(final InputStream in, final StartingFieldStrategy strategy, final String startingFieldName, StreamReadConstraints streamReadConstraints) throws IOException {
        this(in, strategy, startingFieldName, new JsonParserFactory(), streamReadConstraints);
    }

    public JsonRecordSource(final InputStream in, final StartingFieldStrategy strategy, final String startingFieldName, TokenParserFactory tokenParserFactory,
                            StreamReadConstraints streamReadConstraints) throws IOException {
        jsonParser = tokenParserFactory.getJsonParser(in, streamReadConstraints, ALLOW_COMMENTS_ENABLED);
        this.strategy = strategy;

        if (strategy == StartingFieldStrategy.NESTED_FIELD) {
            final SerializedString serializedNestedField = new SerializedString(startingFieldName);
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
