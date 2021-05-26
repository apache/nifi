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
package org.apache.nifi.registry.client;

import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;

import java.io.IOException;
import java.util.List;

/**
 * Client for interacting with snapshots.
 */
public interface FlowSnapshotClient {

    /**
     * Creates a new snapshot/version for the given flow.
     *
     * The snapshot object must have the version populated, and will receive an error if the submitted version is
     * not the next one-up version.
     *
     * @param snapshot the new snapshot
     * @return the created snapshot
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    VersionedFlowSnapshot create(VersionedFlowSnapshot snapshot) throws NiFiRegistryException, IOException;

    /**
     * Gets the snapshot for the given bucket, flow, and version.
     *
     * @param bucketId the bucket id
     * @param flowId the flow id
     * @param version the version
     * @return the snapshot with the given version of the given flow in the given bucket
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    VersionedFlowSnapshot get(String bucketId, String flowId, int version) throws NiFiRegistryException, IOException;

    /**
     * Gets the snapshot for the given flow and version.
     *
     * @param flowId the flow id
     * @param version the version
     * @return the snapshot with the given version of the given flow
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    VersionedFlowSnapshot get(String flowId, int version) throws NiFiRegistryException, IOException;

    /**
     * Gets the latest snapshot for the given flow.
     *
     * @param bucketId the bucket id
     * @param flowId the flow id
     * @return the snapshot with the latest version for the given flow
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    VersionedFlowSnapshot getLatest(String bucketId, String flowId) throws NiFiRegistryException, IOException;

    /**
     * Gets the latest snapshot for the given flow.
     *
     * @param flowId the flow id
     * @return the snapshot with the latest version for the given flow
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    VersionedFlowSnapshot getLatest(String flowId) throws NiFiRegistryException, IOException;

    /**
     * Gets the latest snapshot metadata for the given flow.
     *
     * @param bucketId the bucket id
     * @param flowId the flow id
     * @return the snapshot metadata for the latest version of the given flow
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    VersionedFlowSnapshotMetadata getLatestMetadata(String bucketId, String flowId) throws NiFiRegistryException, IOException;

    /**
     * Gets the latest snapshot metadata for the given flow.
     *
     * @param flowId the flow id
     * @return the snapshot metadata for the latest version of the given flow
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    VersionedFlowSnapshotMetadata getLatestMetadata(String flowId) throws NiFiRegistryException, IOException;

    /**
     * Gets a list of the metadata for all snapshots of a given flow.
     *
     * The contents of each snapshot are not part of the response.
     *
     * @param bucketId the bucket id
     * @param flowId the flow id
     * @return the list of snapshot metadata
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    List<VersionedFlowSnapshotMetadata> getSnapshotMetadata(String bucketId, String flowId) throws NiFiRegistryException, IOException;

    /**
     * Gets a list of the metadata for all snapshots of a given flow.
     *
     * The contents of each snapshot are not part of the response.
     *
     * @param flowId the flow id
     * @return the list of snapshot metadata
     * @throws NiFiRegistryException if an error is encountered other than IOException
     * @throws IOException if an I/O error is encountered
     */
    List<VersionedFlowSnapshotMetadata> getSnapshotMetadata(String flowId) throws NiFiRegistryException, IOException;

}
