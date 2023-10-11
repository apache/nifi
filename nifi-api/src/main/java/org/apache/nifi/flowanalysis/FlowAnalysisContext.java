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
package org.apache.nifi.flowanalysis;

import org.apache.nifi.controller.VersionedControllerServiceLookup;

import java.util.Optional;

/**
 * Used for accessing flow- or other analysis-related information
 */
public interface FlowAnalysisContext {
    /**
     * @return the {@link VersionedControllerServiceLookup} which can be used to obtain
     * Versioned Controller Services during flow analysis
     */
    VersionedControllerServiceLookup getVersionedControllerServiceLookup();

    /**
     * @return the currently configured maximum number of threads that can be
     * used for executing processors at any given time.
     */
    int getMaxTimerDrivenThreadCount();

    /**
     * @return <code>true</code> if this instance of NiFi is configured to be part of a cluster, <code>false</code>
     * if this instance of NiFi is a standalone instance
     */
    boolean isClustered();

    /**
     * @return an Optional with the ID of this node in the cluster, or empty if either this node is not clustered or the Node Identifier
     * has not yet been established
     */
    Optional<String> getClusterNodeIdentifier();
}
