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
package org.apache.nifi.hazelcast.services;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Simple serializer and deserializer for testing purposes.
 */
public final class DummyStringSerializer implements Serializer<String>, Deserializer<String> {
    @Override
    public void serialize(final String value, final OutputStream output) throws SerializationException, IOException {
        output.write(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String deserialize(final byte[] input) throws DeserializationException, IOException {
        return new String(input, StandardCharsets.UTF_8);
    }
}
