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

import org.apache.nifi.schema.inference.RecordSource;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

public class JsonRecordSource implements RecordSource<JsonNode> {
    private static final JsonFactory jsonFactory;
    private final JsonParser jsonParser;

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.setCodec(new ObjectMapper());
    }

    public JsonRecordSource(final InputStream in) throws IOException {
        jsonParser = jsonFactory.createJsonParser(in);
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
        }
    }
}
