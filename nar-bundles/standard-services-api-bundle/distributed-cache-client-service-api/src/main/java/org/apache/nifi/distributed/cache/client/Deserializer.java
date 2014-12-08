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

import org.apache.nifi.distributed.cache.client.exception.DeserializationException;

/**
 * Provides an interface for deserializing an array of bytes into an Object
 *
 * @param <T>
 */
public interface Deserializer<T> {

    /**
     * Deserializes the given byte array input an Object and returns that value.
     *
     * @param input
     * @return
     * @throws DeserializationException if a valid object cannot be deserialized
     * from the given byte array
     * @throws java.io.IOException
     */
    T deserialize(byte[] input) throws DeserializationException, IOException;

}
