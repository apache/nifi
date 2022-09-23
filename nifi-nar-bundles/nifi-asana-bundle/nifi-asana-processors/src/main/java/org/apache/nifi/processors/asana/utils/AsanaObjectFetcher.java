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
package org.apache.nifi.processors.asana.utils;

import java.util.Map;

/**
 * This interface defines a common API to fetch objects from Asana without knowing what kind of objects are these,
 * and how to process them. Implementors responsibility to detect whether an object is new, updated, or removed.
 */
public interface AsanaObjectFetcher {
    /**
     * Call this method to retrieve the next unprocessed object from Asana. The returned {@link AsanaObject} contains
     * the object in Json formatted string, and information about its ID and whether it is new, updated, or removed.
     *
     * @return null if there are no more unprocessed objects at the moment, {@link AsanaObject} otherwise.
     */
    AsanaObject fetchNext();

    /**
     * Call this method to serialize this object fetcher's state.
     *
     * @return A {@link Map} containing the object state in key-value pairs. Optimized for using it with
     *         {@link org.apache.nifi.components.state.StateManager}
     */
    Map<String, String> saveState();

    /**
     * Call this method to deserialize & restore a state that was saved/exported earlier.
     *
     * @param state A {@link Map} containing all the key-value pairs returned by a prior call to {@code saveState()}.
     */
    void loadState(Map<String, String> state);

    /**
     * Call this method to wipe out all the state information of this object fetcher. As if it was brand-new.
     */
    void clearState();
}
