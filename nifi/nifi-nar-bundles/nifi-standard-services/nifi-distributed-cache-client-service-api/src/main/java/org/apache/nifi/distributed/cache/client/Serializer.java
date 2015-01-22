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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.distributed.cache.client.exception.SerializationException;

/**
 * Provides a mechanism by which a value can be serialized to a stream of bytes
 * @param <T>
 */
public interface Serializer<T> {

    /**
     * Serializes the given value to the {@link OutputStream}
     *
     * @param value
     * @param output
     * @throws SerializationException If unable to serialize the given value
     * @throws java.io.IOException
     */
    void serialize(T value, OutputStream output) throws SerializationException, IOException;

}
