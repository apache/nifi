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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

public class GenericObjectSerDe <V> implements Serializer<V>, Deserializer<V> {

    @Override
    @SuppressWarnings("unchecked")
    public V deserialize(byte[] value) throws DeserializationException, IOException {
        if (value == null || value.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(value)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(bis)) {
                return (V) objectInputStream.readObject();
            } catch (ClassNotFoundException e) {
                throw new DeserializationException(e);
            }
        }
    }

    @Override
    public void serialize(V value, OutputStream output) throws SerializationException, IOException {
        if (value == null) {
            return;
        }

        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(output)) {
            objectOutputStream.writeObject(value);
        }
    }
}
