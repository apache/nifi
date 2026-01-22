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

package org.apache.nifi.components.connector;

import org.apache.nifi.flow.Bundle;

import java.util.List;
import java.util.Optional;

/**
 * Provides the ability to look up available bundles for a given component type.
 */
public interface ComponentBundleLookup {

    /**
     * Returns the available bundles that provide the given component type.
     *
     * @param componentType the fully qualified class name of the component type
     * @return the list of bundles that provide the component type
     */
    List<Bundle> getAvailableBundles(String componentType);

    /**
     * Returns the latest version of a bundle that provides the given component type.
     *
     * @param componentType the fully qualified class name of the component type
     * @return an Optional containing the latest bundle, or empty if no bundles are available
     */
    Optional<Bundle> getLatestBundle(String componentType);
}
