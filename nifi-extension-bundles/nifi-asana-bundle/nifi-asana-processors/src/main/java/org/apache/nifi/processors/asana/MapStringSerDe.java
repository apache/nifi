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
package org.apache.nifi.processors.asana;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class MapStringSerDe implements Serializer<Map<String, String>>, Deserializer<Map<String, String>> {

    private static final Charset CHARACTER_SET = StandardCharsets.UTF_8;

    private static final Gson GSON = new Gson();

    private static final TypeToken<Map<String, String>> MAP_TYPE_TOKEN = new TypeToken<>() { };

    @Override
    public Map<String, String> deserialize(final byte[] value) throws DeserializationException, IOException {
        if (value == null || value.length == 0) {
            return null;
        }

        try (Reader reader = new InputStreamReader(new ByteArrayInputStream(value), CHARACTER_SET)) {
            return GSON.fromJson(reader, MAP_TYPE_TOKEN);
        }
    }

    @Override
    public void serialize(final Map<String, String> value, final OutputStream output) throws SerializationException, IOException {
        if (value == null) {
            return;
        }

        try (Writer writer = new OutputStreamWriter(output, CHARACTER_SET)) {
            GSON.toJson(value, writer);
        }
    }
}
