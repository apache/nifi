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
package org.apache.nifi.processors.standard.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

/**
 * Offers serialization and deserialization for flow file attribute maps.
 *
 */
public class FlowFileAttributesSerializer implements Deserializer<Map<String, String>>, Serializer<Map<String, String>> {

    private static final String ATTRIBUTE_SEPARATOR = "<|--|>";

    @Override
    public Map<String, String> deserialize(final byte[] input) throws DeserializationException, IOException {
        if (input == null || input.length == 0) {
            return null;
        }
        Map<String, String> attributes = new HashMap<>();

        String attributesText = new String(input, StandardCharsets.UTF_8);
        String[] entries = attributesText.split(Pattern.quote(ATTRIBUTE_SEPARATOR));
        for(String entry : entries) {
            int equalsIndex = entry.indexOf('=');
            String key = entry.substring(0, equalsIndex);
            String value = entry.substring(equalsIndex + 1);
            attributes.put(key, value);
        }

        return attributes;
    }

    @Override
    public void serialize(Map<String, String> value, OutputStream output) throws SerializationException, IOException {
        int i = 0;
        for(Entry<String, String> entry : value.entrySet()) {
            output.write((entry.getKey() + '=' + entry.getValue()).getBytes(StandardCharsets.UTF_8));
            if (i < value.size() - 1) {
                output.write(ATTRIBUTE_SEPARATOR.getBytes(StandardCharsets.UTF_8));
            }
            i++;
        }
        output.flush();
    }
}
