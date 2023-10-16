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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

public class JsonParserFactory implements TokenParserFactory{
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @Override
    public JsonParser getJsonParser(InputStream in) throws IOException {
        JsonParser jsonParser = JSON_FACTORY.createParser(in);
        jsonParser.setCodec(JSON_MAPPER);

        return jsonParser;
    }

    @Override
    public ObjectMapper createCodec(boolean allowComments, StreamReadConstraints streamReadConstraints) {
        ObjectMapper codec = new ObjectMapper();
        if(allowComments) {
            codec.enable(JsonParser.Feature.ALLOW_COMMENTS);
        }
        codec.getFactory().setStreamReadConstraints(streamReadConstraints);

        return codec;
    }
}
