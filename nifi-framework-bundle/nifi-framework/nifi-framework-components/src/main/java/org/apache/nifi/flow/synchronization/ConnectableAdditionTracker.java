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

package org.apache.nifi.flow.synchronization;

import org.apache.nifi.connectable.Connectable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ConnectableAdditionTracker {
    private static final Logger logger = LoggerFactory.getLogger(ConnectableAdditionTracker.class);

    private final Map<ComponentKey, Connectable> tracking = new HashMap<>();

    public void addComponent(final String instantiatedGroupId, final String versionedComponentId, final Connectable component) {
        final ComponentKey key = new ComponentKey(instantiatedGroupId, versionedComponentId);
        if (tracking.containsKey(key)) {
            logger.debug("Component [{}] and Versioned Component ID [{}] added to Process Group [{}] but component with same Versioned ID already added",
                component, versionedComponentId, instantiatedGroupId);
        }

        tracking.putIfAbsent(key, component);
    }

    public Optional<Connectable> getComponent(final String instantiatedGroupId, final String versionedComponentId) {
        final ComponentKey key = new ComponentKey(instantiatedGroupId, versionedComponentId);
        return Optional.ofNullable(tracking.get(key));
    }

    private static class ComponentKey {
        private final String instantiatedGroupId;
        private final String versionedComponentId;

        public ComponentKey(final String instantiatedGroupid, final String versionedComponentId) {
            this.instantiatedGroupId = instantiatedGroupid;
            this.versionedComponentId = versionedComponentId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ComponentKey that = (ComponentKey) o;
            return Objects.equals(instantiatedGroupId, that.instantiatedGroupId)
                && Objects.equals(versionedComponentId, that.versionedComponentId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(instantiatedGroupId, versionedComponentId);
        }
    }
}
