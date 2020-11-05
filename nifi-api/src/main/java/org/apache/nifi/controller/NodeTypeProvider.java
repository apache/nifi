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

package org.apache.nifi.controller;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * <p>
 * This interface provides a set of methods for checking NiFi node type.
 * <p>
 */
public interface NodeTypeProvider {

    /**
     * @return true if this instance is clustered, false otherwise.MockProcessContext
     * Clustered means that a node is either connected or trying to connect to the cluster.
     */
    boolean isClustered();

    /**
     * @return true if the expected state of clustering is true, false otherwise. Contrary to {{@link #isClustered()}}
     * this does not dynamically change with the state of this node.
     */
    default boolean isConfiguredForClustering() {
        return false;
    }

    /**
     * @return true if this instances is clustered and connected to the cluster.
     */
    default boolean isConnected() {
        return false;
    }

    /**
     * @return true if this instance is the primary node in the cluster; false otherwise
     */
    boolean isPrimary();

    /**
     * @return Returns with the hostname of the current node, if clustered. For For a standalone
     * NiFi this returns an empty instead.
     */
    default Optional<String> getCurrentNode() {
        if (isClustered()) {
            throw new IllegalStateException("Clustered environment is not handled!");
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return Names/IP addresses of all expected hosts in the cluster (including the current one). For a standalone
     * NiFi this returns an empty set instead.
     */
    default Set<String> getClusterMembers() {
        if (isClustered()) {
            throw new IllegalStateException("Clustered environment is not handled!");
        } else {
            return Collections.emptySet();
        }
    }
}
