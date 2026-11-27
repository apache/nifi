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
package org.apache.nifi.web;

import org.apache.nifi.web.api.entity.AffectedComponentEntity;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class FlowUpdateImpact {
    private final Set<AffectedComponentEntity> affectedComponents;
    private final Set<RemovedConnectionDescriptor> removedConnections;
    private final Set<String> removedProcessGroupIds;
    private final Set<String> removedEndpointIds;

    public FlowUpdateImpact(final Set<AffectedComponentEntity> affectedComponents,
                            final Set<RemovedConnectionDescriptor> removedConnections,
                            final Set<String> removedProcessGroupIds,
                            final Set<String> removedEndpointIds) {
        this.affectedComponents = unmodifiableCopy(affectedComponents);
        this.removedConnections = unmodifiableCopy(removedConnections);
        this.removedProcessGroupIds = unmodifiableCopy(removedProcessGroupIds);
        this.removedEndpointIds = unmodifiableCopy(removedEndpointIds);
    }

    public Set<AffectedComponentEntity> getAffectedComponents() {
        return affectedComponents;
    }

    public Set<RemovedConnectionDescriptor> getRemovedConnections() {
        return removedConnections;
    }

    public Set<String> getRemovedProcessGroupIds() {
        return removedProcessGroupIds;
    }

    public Set<String> getRemovedEndpointIds() {
        return removedEndpointIds;
    }

    private static <T> Set<T> unmodifiableCopy(final Set<T> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }

        return Collections.unmodifiableSet(new LinkedHashSet<>(values));
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof final FlowUpdateImpact other)) {
            return false;
        }

        return Objects.equals(affectedComponents, other.affectedComponents)
                && Objects.equals(removedConnections, other.removedConnections)
                && Objects.equals(removedProcessGroupIds, other.removedProcessGroupIds)
                && Objects.equals(removedEndpointIds, other.removedEndpointIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(affectedComponents, removedConnections, removedProcessGroupIds, removedEndpointIds);
    }
}
