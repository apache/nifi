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

package org.apache.nifi.registry.flow.mapping;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

public interface ComponentIdLookup {

    /**
     * Given a component identifier and an optional Versioned Component ID, returns the identifier to use for the component
     * @param currentVersionedId the current Versioned Component ID, or an empty optional if the component does not currently have a Versioned Component ID
     * @param componentId the ID of the component
     * @return the ID to use for mapping a component to a Versioned Component
     */
    String getComponentId(Optional<String> currentVersionedId, String componentId);


    /**
     * Uses the Versioned Component ID, if it is present, or else generates a new Versioned Component ID based on the Component ID
     */
    ComponentIdLookup VERSIONED_OR_GENERATE = new ComponentIdLookup() {
        @Override
        public String getComponentId(final Optional<String> currentVersionedId, final String componentId) {
            if (currentVersionedId.isPresent()) {
                return currentVersionedId.get();
            }

            return UUID.nameUUIDFromBytes(componentId.getBytes(StandardCharsets.UTF_8)).toString();
        }
    };



    /**
     * Always uses the Component ID
     */
    ComponentIdLookup USE_COMPONENT_ID = (versioned, componentId) -> componentId;
}
