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
package org.apache.nifi.distributed.cache.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

/**
 * Utility class to abstract serialization logic from network byte stream operations.
 */
public class CacheClientSerde {

    /**
     * Serialize a value of the given type.
     *
     * @param value      the value to be serialized
     * @param serializer the serializer for the input value
     * @param <T>        the value type
     * @return the byte stream representation of the input value
     * @throws IOException on serialization failure
     */
    public static <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        serializer.serialize(value, os);
        return os.toByteArray();
    }

    /**
     * Serialize a collection of values of the given type.
     *
     * @param values     the values to be serialized
     * @param serializer the serializer for the input values
     * @param <T>        the value type
     * @return a collection of the byte stream representations of the input values
     * @throws IOException on serialization failure
     */
    public static <T> Collection<byte[]> serialize(final Set<T> values, final Serializer<T> serializer) throws IOException {
        final Collection<byte[]> bytesValues = new ArrayList<>();
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (T value : values) {
            serializer.serialize(value, os);
            bytesValues.add(os.toByteArray());
            os.reset();
        }
        return bytesValues;
    }
}
