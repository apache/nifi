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

import java.io.IOException;
import java.util.Map;

/**
 * <p>
 * The StateManager is responsible for providing NiFi components a mechanism for storing
 * and retrieving state.
 * </p>
 *
 * <p>
 * When calling methods in this class, the {@link Scope} is used in order to specify whether
 * state should be stored/retrieved from the local state or the clustered state. However, if
 * any instance of NiFi is not clustered (or is disconnected from its cluster), the Scope is
 * not really relevant and the local state will be used in all cases. This allows component
 * developers to not concern themselves with whether or not a particular instance of NiFi is
 * clustered. Instead, developers should assume that the instance is indeed clustered and write
 * the component accordingly. If not clustered, the component will still behavior in the same
 * manner, as a standalone node could be thought of as a "cluster of 1."
 * </p>
 */
public interface StateManager {

    /**
     * Updates the value of the component's state, setting it to given value
     *
     * @param state the value to change the state to
     * @param scope the scope to use when storing the state
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    void setState(Map<String, String> state, Scope scope) throws IOException;

    /**
     * Returns the current state for the component. This return value will never be <code>null</code>.
     * If the state has not yet been set, the StateMap's version will be -1, and the map of values will be empty.
     *
     * @param scope the scope to use when fetching the state
     * @return
     * @throws IOException
     */
    StateMap getState(Scope scope) throws IOException;

    /**
     * Updates the value of the component's state to the new value if and only if the value currently
     * is the same as the given oldValue.
     *
     * @param oldValue the old value to compare against
     * @param newValue the new value to use if and only if the state's value is the same as the given oldValue
     * @param scope the scope to use for storing the new state
     * @return <code>true</code> if the state was updated to the new value, <code>false</code> if the state's value was not
     *         equal to oldValue
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    boolean replace(StateMap oldValue, Map<String, String> newValue, Scope scope) throws IOException;

    /**
     * Clears all keys and values from the component's state
     *
     * @param scope the scope whose values should be cleared
     *
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    void clear(Scope scope) throws IOException;
}
