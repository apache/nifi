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
package org.apache.nifi.registry.cluster;

import java.util.Objects;

/**
 * Immutable value object identifying a cluster member by its node identifier
 * and HTTP base URL (e.g. {@code https://node1:18443}).
 */
public final class NodeAddress {

    private final String nodeId;
    private final String baseUrl;

    public NodeAddress(final String nodeId, final String baseUrl) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId is required");
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl is required");
    }

    /** The logical node identifier (matches {@code nifi.registry.cluster.node.identifier}). */
    public String getNodeId() {
        return nodeId;
    }

    /** The HTTP base URL used for inter-node replication (e.g. {@code https://node1:18443}). */
    public String getBaseUrl() {
        return baseUrl;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NodeAddress that = (NodeAddress) o;
        return Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "NodeAddress{nodeId='" + nodeId + "', baseUrl='" + baseUrl + "'}";
    }
}
