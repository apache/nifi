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

package org.apache.nifi.provenance;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides a mechanism for obtaining the identifiers of components, queues, etc.
 */
public interface IdentifierLookup {

    /**
     * @return the identifiers of components that may generate Provenance Events
     */
    List<String> getComponentIdentifiers();

    /**
     * @return a list of component types that may generate Provenance Events
     */
    List<String> getComponentTypes();

    /**
     *
     * @return the identifiers of FlowFile Queues that are in the flow
     */
    List<String> getQueueIdentifiers();

    default Map<String, Integer> invertQueueIdentifiers() {
        return invertList(getQueueIdentifiers());
    }

    default Map<String, Integer> invertComponentTypes() {
        return invertList(getComponentTypes());
    }

    default Map<String, Integer> invertComponentIdentifiers() {
        return invertList(getComponentIdentifiers());
    }

    default Map<String, Integer> invertList(final List<String> values) {
        final Map<String, Integer> inverted = new HashMap<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            inverted.put(values.get(i), i);
        }
        return inverted;
    }


    public static final IdentifierLookup EMPTY = new IdentifierLookup() {
        @Override
        public List<String> getComponentIdentifiers() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getComponentTypes() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getQueueIdentifiers() {
            return Collections.emptyList();
        }

        @Override
        public Map<String, Integer> invertList(List<String> values) {
            return Collections.emptyMap();
        }
    };
}
