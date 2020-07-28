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

package org.apache.nifi.lookup;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.nifi.controller.ControllerService;

public interface LookupService<T> extends ControllerService {

    /**
     * Looks up a value that corresponds to the given map of information, referred to as lookup coordinates
     *
     * @param coordinates a Map of key/value pairs that indicate the information that should be looked up
     * @return a value that corresponds to the given coordinates
     *
     * @throws LookupFailureException if unable to lookup a value for the given coordinates
     */
    Optional<T> lookup(Map<String, Object> coordinates) throws LookupFailureException;

    /**
     * Looks up a value that corresponds to the given map, coordinates. Additional contextual information will also be passed into the
     * map labeled context from sources such as flowfile attributes.
     *
     * @param coordinates a Map of key/value pairs that indicate the information that should be looked up
     * @param context a Map of additional information
     * @return a value that corresponds to the given coordinates
     * @throws LookupFailureException if unable to lookup a value for the given coordinates
     */
    default Optional<T> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        return lookup(coordinates);
    }

    /**
     * @return the Class that represents the type of value that will be returned by {@link #lookup(Map)}
     */
    Class<?> getValueType();

    /**
     * Many Lookup Services will require a specific set of information be passed in to the {@link #lookup(Map)} method.
     * This method will return the Set of keys that must be present in the map that is passed to {@link #lookup(Map)} in order
     * for the lookup to succeed.
     *
     * @return the keys that must be present in the map passed to {@link #lookup(Map)} in order to the lookup to succeed, or an empty set
     *         if no specific keys are required.
     */
    Set<String> getRequiredKeys();
}
