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
package org.apache.nifi.redis.state;

import java.io.IOException;

/**
 * Provides serialization/deserialization of a RedisStateMap.
 */
public interface RedisStateMapSerDe {

    /**
     * Serializes the given RedisStateMap.
     *
     * @param stateMap the RedisStateMap to serialize
     * @return the serialized bytes or null if stateMap is null
     * @throws IOException if an error occurs when serializing
     */
    byte[] serialize(RedisStateMap stateMap) throws IOException;

    /**
     * Deserializes the given bytes to a RedisStateMap.
     *
     * @param data bytes previously stored by RedisStateProvider
     * @return a RedisStateMap or null if data is null or length 0
     * @throws IOException if an error occurs when deserializing
     */
    RedisStateMap deserialize(byte[] data) throws IOException;

}
