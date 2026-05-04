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
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class JsonParserFactory implements TokenParserFactory {
    private static final ObjectMapper defaultObjectMapper = new ObjectMapper();

    private final JsonFactory jsonFactory;

    /**
     * JSON Parser Factory constructor using default ObjectMapper and associated configuration options
     */
    public JsonParserFactory() {
        jsonFactory = defaultObjectMapper.getFactory();
    }

    /**
     * JSON Parser Factory constructor with configurable constraints
     *
     * @param streamReadConstraints Stream Read Constraints
     * @param lenient Allow for parsing JSON leniently
     */
    public JsonParserFactory(final StreamReadConstraints streamReadConstraints, final boolean lenient) {
        Objects.requireNonNull(streamReadConstraints, "Stream Read Constraints required");

        final ObjectMapper objectMapper = new ObjectMapper();
        if (lenient) {
            objectMapper.enable(JsonReadFeature.ALLOW_JAVA_COMMENTS.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_YAML_COMMENTS.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_SINGLE_QUOTES.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_LEADING_PLUS_SIGN_FOR_NUMBERS.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_LEADING_ZEROS_FOR_NUMBERS.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_TRAILING_DECIMAL_POINT_FOR_NUMBERS.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_TRAILING_COMMA.mappedFeature());
            objectMapper.enable(JsonReadFeature.ALLOW_MISSING_VALUES.mappedFeature());
        }
        jsonFactory = objectMapper.getFactory();
        jsonFactory.setStreamReadConstraints(streamReadConstraints);
    }

    @Override
    public JsonParser getJsonParser(final InputStream in) throws IOException {
        Objects.requireNonNull(in, "Input Stream required");
        return jsonFactory.createParser(in);
    }
}
