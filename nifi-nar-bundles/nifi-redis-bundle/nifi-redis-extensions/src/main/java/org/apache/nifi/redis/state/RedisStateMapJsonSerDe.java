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
package org.apache.nifi.redis.state;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * A RedisStateMapSerDe that uses JSON as the underlying representation.
 */
public class RedisStateMapJsonSerDe implements RedisStateMapSerDe {

    public static final String FIELD_VERSION = "version";
    public static final String FIELD_ENCODING = "encodingVersion";
    public static final String FIELD_STATE_VALUES = "stateValues";

    private final JsonFactory jsonFactory = new JsonFactory();

    @Override
    public byte[] serialize(final RedisStateMap stateMap) throws IOException {
        if (stateMap == null) {
            return null;
        }

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final JsonGenerator jsonGenerator = jsonFactory.createGenerator(out);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeNumberField(FIELD_VERSION, stateMap.getVersion());
            jsonGenerator.writeNumberField(FIELD_ENCODING, stateMap.getEncodingVersion());

            jsonGenerator.writeObjectFieldStart(FIELD_STATE_VALUES);
            for (Map.Entry<String,String> entry : stateMap.toMap().entrySet()) {
                jsonGenerator.writeStringField(entry.getKey(), entry.getValue());
            }
            jsonGenerator.writeEndObject();

            jsonGenerator.writeEndObject();
            jsonGenerator.flush();

            return out.toByteArray();
        }
    }

    @Override
    public RedisStateMap deserialize(final byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return null;
        }

        final RedisStateMap.Builder builder = new RedisStateMap.Builder();

        try (final JsonParser jsonParser = jsonFactory.createParser(data)) {
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    final String fieldName = jsonParser.getCurrentName();

                    switch (fieldName) {
                        case FIELD_VERSION:
                            jsonParser.nextToken();
                            builder.version(jsonParser.getLongValue());
                            break;
                        case FIELD_ENCODING:
                            jsonParser.nextToken();
                            builder.encodingVersion(jsonParser.getIntValue());
                            break;
                        case FIELD_STATE_VALUES:
                            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                                if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
                                    final String stateValueField = jsonParser.getCurrentName();
                                    jsonParser.nextToken();
                                    builder.stateValue(stateValueField, jsonParser.getValueAsString());
                                }
                            }
                            break;
                    }
                }
            }
        }

        return builder.build();
    }

}
