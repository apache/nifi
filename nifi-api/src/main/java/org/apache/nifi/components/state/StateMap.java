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

package org.apache.nifi.components.state;

import java.util.Map;

/**
 * Provides a representation of a component's state at some point in time.
 */
public interface StateMap {
    /**
     * Each time that a component's state is updated, the state is assigned a new version.
     * This version can then be used to atomically update state by the backing storage mechanism.
     * Though this number is monotonically increasing, it should not be expected to increment always
     * from X to X+1. I.e., version numbers may be skipped.
     *
     * @return the version associated with the state
     */
    long getVersion();

    /**
     * Returns the value associated with the given key
     *
     * @param key the key whose value should be retrieved
     * @return the value associated with the given key, or <code>null</code> if no value is associated
     *         with this key.
     */
    String get(String key);

    /**
     * Returns an immutable Map representation of all keys and values for the state of a component.
     *
     * @return an immutable Map representation of all keys and values for the state of a component.
     */
    Map<String, String> toMap();
}
