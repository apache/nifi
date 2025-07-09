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
package org.apache.nifi.couchbase;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CouchbaseUtils {

    /**
     * A convenient method to retrieve String value when Document type is unknown.
     * This method uses LegacyDocument to get, then tries to convert content based on its class.
     * @param collection the collection to get a document from
     * @param id the id of the target document
     * @return String representation of the stored value, or null if not found
     */
    public static String getStringContent(Collection collection, String id) {
        try {
            return new String(collection.get(id).contentAsBytes());
        } catch (DocumentNotFoundException dnfe) {
            return null;
        }
    }

    public static String getStringContent(Object content) {
        if (content instanceof String) {
            return (String) content;
        } else if (content instanceof byte[]) {
            return new String((byte[]) content, StandardCharsets.UTF_8);
        } else if (content instanceof ByteBuffer) {
            final ByteBuffer byteBuf = (ByteBuffer) content;
            byte[] bytes = byteBuf.array();
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return content.toString();
    }

    /**
     * A method to get sub-document paths from a JsonObject
     * @param doc source object
     * @param subDocPath dot-separated property path for the return value
     * @return value at subDocPath
     * @throws Exception will pass any exception up the stack
     */
    public static String getSubDocPath(JsonObject doc, String subDocPath) throws Exception {
        JsonObject last = doc;
        String[] parts = subDocPath.split("\\.");
        for (int part = 0; part < parts.length; part++) {
            String nextPath = parts[part];
            int idx = nextPath.lastIndexOf('[');
            if (idx == 0) {
                throw new IllegalArgumentException("Invalid subdoc path, no subelement name provided before array index: " + subDocPath);
            }
            if (idx > -1) {
                String nextName = nextPath.substring(0, idx);
                if (!nextPath.endsWith("]")) {
                    throw new IllegalArgumentException("Invalid subdoc path, missing closing bracket for array index: " + subDocPath);
                }
                int nextIdx = Integer.valueOf(nextPath.substring(idx + 1, nextPath.length() - 1));
                JsonArray nextArray = last.getArray(nextName);
                if (part == parts.length - 1) {
                    return nextArray.get(nextIdx).toString();
                }
                last = nextArray.getObject(nextIdx);
            } else {
                if (part == parts.length - 1) {
                    return last.getString(nextPath);
                }
                last = last.getObject(nextPath);
            }
        }

        return null;
    }

    public static <V> Transcoder transcoder(Serializer<V> serializer, Deserializer<V> deserializer) {
        return new Transcoder() {

            @Override
            public EncodedValue encode(Object o) {
                ByteArrayOutputStream serialized = new ByteArrayOutputStream();
                try {
                    serializer.serialize((V) o, serialized);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new EncodedValue(serialized.toByteArray(), 0);
            }

            @Override
            public <T> T decode(Class<T> aClass, byte[] bytes, int i) {
                try {
                    return (T) deserializer.deserialize(bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
