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
package org.apache.nifi.c2.client.api;

import java.util.Optional;

/**
 * Helper class to support central configuration and functionality for serialisation / deserialisation
 */
public interface C2Serializer {

    /**
     * Helper to serialise object
     *
     * @param content object to be serialised
     * @param <T> the type of the object
     * @return the serialised string representation of the parameter object if it was successful empty otherwise
     */
    <T> Optional<String> serialize(T content);

    /**
     * Helper to deserialise an object
     *
     * @param content the string representation of the object to be deserialsed
     * @param valueType the class of the target object
     * @param <T> the type of the target object
     * @return the deserialised object if successful empty otherwise
     */
    <T> Optional<T> deserialize(String content, Class<T> valueType);
}
