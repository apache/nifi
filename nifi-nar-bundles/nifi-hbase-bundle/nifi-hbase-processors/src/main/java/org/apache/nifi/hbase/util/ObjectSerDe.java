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
package org.apache.nifi.hbase.util;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class ObjectSerDe implements Serializer<Object>, Deserializer<Object> {

    @Override
    public Object deserialize(byte[] input) throws DeserializationException, IOException {
        if (input == null || input.length == 0) {
            return null;
        }

        try (final ByteArrayInputStream in = new ByteArrayInputStream(input);
            final ObjectInputStream objIn = new ObjectInputStream(in)) {
            return objIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new DeserializationException("Could not deserialize object due to ClassNotFoundException", e);
        }
    }

    @Override
    public void serialize(Object value, OutputStream output) throws SerializationException, IOException {
        try (final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                final ObjectOutputStream objOut = new ObjectOutputStream(bOut)) {
            objOut.writeObject(value);
            output.write(bOut.toByteArray());
        }
    }

}
