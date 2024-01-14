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

package org.apache.nifi.processor.util.list;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.stream.io.GZIPOutputStream;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPInputStream;

class DistributedMapCacheClientSerialization {
    static final Serializer<String> stringSerializer =
            (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));
    static final Deserializer<String> stringDeserializer =
            value -> value == null ? null : new String(value, StandardCharsets.UTF_8);

    private static final ObjectMapper objectMapper = new ObjectMapper();
    static final Serializer<Map<String, ListedEntity>> listedEntitiesSerializer = (value, out) -> {
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out);
        objectMapper.writeValue(gzipOutputStream, value);
        // Finish writing gzip data without closing the underlying stream.
        gzipOutputStream.finish();
    };
    static final Deserializer<Map<String, ListedEntity>> listedEntitiesDeserializer = value -> {
        if (value == null || value.length == 0) {
            return null;
        }
        try (final GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(value))) {
            return objectMapper.readValue(in, new TypeReference<Map<String, ListedEntity>>() {});
        }
    };
}