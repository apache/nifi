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

import java.util.Optional;

import org.apache.nifi.controller.ControllerService;

public interface LookupService<T> extends ControllerService {

    /**
     * Looks up a value that corresponds to the given key
     *
     * @param key the key to lookup
     * @return a value that corresponds to the given key
     *
     * @throws LookupFailureException if unable to lookup a value for the given key
     */
    Optional<T> lookup(String key) throws LookupFailureException;

    /**
     * @return the Class that represents the type of value that will be returned by {@link #lookup(String)}
     */
    Class<?> getValueType();

}
