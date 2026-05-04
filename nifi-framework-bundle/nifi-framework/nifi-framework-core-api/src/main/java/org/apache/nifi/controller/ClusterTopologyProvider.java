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

/**
 * Framework-internal helper that exposes connected, port-disambiguated cluster topology information
 * needed for ordinal-based capture and restoration of component state. Standalone NiFi instances
 * report ordinal {@code 0} and a count of {@code 1}.
 */
public interface ClusterTopologyProvider {

    /**
     * Returns the ordinal index of the local node among connected cluster nodes when sorted
     * deterministically by API address and API port. Standalone NiFi returns {@code 0}.
     *
     * @return the ordinal index of the local node, or {@code 0} when not clustered
     */
    int getLocalNodeOrdinal();

    /**
     * Returns the number of currently connected cluster nodes. Standalone NiFi returns {@code 1}.
     *
     * @return the number of connected cluster nodes, or {@code 1} when not clustered
     */
    int getConnectedNodeCount();
}
