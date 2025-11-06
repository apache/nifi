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
package org.apache.nifi.stateless.core;

import org.apache.nifi.registry.flow.VersionedFlowSnapshot;

import java.io.IOException;

/**
 * Abstraction for retrieving Versioned Flow Snapshot using provided coordinates
 */
public interface FlowSnapshotProvider {
    /**
     * Get Versioned Flow Snapshot with specified version
     *
     * @param bucketId Bucket Identifier
     * @param flowId Flow Identifier
     * @param version Version number or -1 indicating latest version
     * @return Versioned Flow Snapshot or null when not found
     * @throws IOException Thrown on failure to retrieving Versioned Flow Snapshot
     */
    VersionedFlowSnapshot getFlowSnapshot(String bucketId, String flowId, int version) throws IOException;
}
