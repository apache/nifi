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
package org.apache.nifi.registry.serialization;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializes and de-serializes objects.
 */
public interface Serializer<T> {

    /**
     * Serializes a snapshot to the given output stream.
     *
     * @param t the object to serialize
     * @param out the output stream to serialize to
     */
    void serialize(T t, OutputStream out) throws SerializationException;

    /**
     * Deserializes the given InputStream back to an object of the given type.
     *
     * @param input the InputStream to deserialize
     * @return the deserialized object
     */
    T deserialize(InputStream input) throws SerializationException;

}
