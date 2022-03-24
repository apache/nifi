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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/**
 * This interface defines an API that can be used for interacting with a
 * Distributed Cache that functions similarly to a {@link java.util.Set Set}.
 */
@Tags({"distributed", "client", "cluster", "set", "cache"})
@CapabilityDescription("Provides the ability to communicate with a DistributedSetCacheServer. This allows "
        + "multiple nodes to coordinate state with a single remote entity.")
public interface DistributedSetCacheClient extends ControllerService {

    /**
     * Adds the specified value to the cache, serializing the value with the
     * given {@link Serializer}.
     *
     * @param <T> type
     * @param value value
     * @param serializer serializer
     * @return true if the value was added to the cache, false if the value
     * already existed in the cache
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    <T> boolean addIfAbsent(T value, Serializer<T> serializer) throws IOException;

    /**
     * @param <T> type
     * @param value value
     * @param serializer serializer
     * @return if the given value is present in the cache and if so returns
     * <code>true</code>, else returns <code>false</code>
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    <T> boolean contains(T value, Serializer<T> serializer) throws IOException;

    /**
     * Removes the given value from the cache, if it is present.
     *
     * @param <T> type
     * @param value value
     * @param serializer serializer
     * @return <code>true</code> if the value is removed, <code>false</code> if
     * the value did not exist in the cache
     * @throws IOException ex
     */
    <T> boolean remove(T value, Serializer<T> serializer) throws IOException;

    /**
     * Attempts to notify the server that we are finished communicating with it
     * and cleans up resources
     *
     * @throws java.io.IOException ex
     */
    void close() throws IOException;
}
