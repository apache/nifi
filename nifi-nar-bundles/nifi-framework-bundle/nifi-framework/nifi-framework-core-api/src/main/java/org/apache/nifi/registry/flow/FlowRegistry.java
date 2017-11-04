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

package org.apache.nifi.registry.flow;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryException;

import java.io.IOException;
import java.util.Set;

public interface FlowRegistry {
    /**
     * @return the ID of the Flow Registry
     */
    String getIdentifier();

    /**
     * @return the description of the Flow Registry
     */
    String getDescription();

    /**
     * Updates the Flow Registry's description
     *
     * @param description the description of the Flow Registry
     */
    void setDescription(String description);

    /**
     * @return the URL of the Flow Registry
     */
    String getURL();

    /**
     * Updates the Flow Registry's URL
     *
     * @param url the URL of the Flow Registry
     */
    void setURL(String url);

    /**
     * @return the name of the Flow Registry
     */
    String getName();

    /**
     * Updates the name of the Flow Registry
     *
     * @param name the name of the Flow Registry
     */
    void setName(String name);

    /**
     * Gets the buckets for the specified user.
     *
     * @param user current user
     * @return buckets for this user
     */
    Set<Bucket> getBuckets(NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Gets the bucket with the given ID
     *
     * @param bucketId the id of the bucket
     * @param user user on whose behalf the request is being made
     * @return the bucket with the given ID
     */
    Bucket getBucket(String bucketId, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the set of all Versioned Flows for the specified bucket
     *
     * @param bucketId the ID of the bucket
     * @param user the user on whose behalf the request is being made
     * @return the set of all Versioned Flows for the specified bucket
     * @throws IOException if unable to communicate with the Flow Registry
     * @throws NiFiRegistryException if unable to find the bucket with the given ID or the flow with the given ID
     */
    Set<VersionedFlow> getFlows(String bucketId, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the set of all versions of the specified flow
     *
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     * @param user the user on whose behalf the request is being made
     * @return the set of all versions of the specified flow
     * @throws IOException if unable to communicate with the Flow Registry
     * @throws NiFiRegistryException if unable to find the bucket with the given ID or the flow with the given ID
     */
    Set<VersionedFlowSnapshotMetadata> getFlowVersions(String bucketId, String flowId, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Registers the given Versioned Flow with the Flow Registry
     *
     * @param flow the Versioned Flow to add to the registry
     * @return the fully populated VersionedFlow
     *
     * @throws NullPointerException if the VersionedFlow is null, or if its bucket identifier or name is null
     * @throws NiFiRegistryException if the bucket id does not exist
     */
    VersionedFlow registerVersionedFlow(VersionedFlow flow, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Deletes the specified flow from the Flow Registry
     *
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     */
    VersionedFlow deleteVersionedFlow(String bucketId, String flowId, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Adds the given snapshot to the Flow Registry for the given flow
     *
     * @param flow the Versioned Flow
     * @param snapshot the snapshot of the flow
     * @param comments any comments for the snapshot
     * @param expectedVersion the version of the flow that we expect to save this snapshot as
     * @return the versioned flow snapshot
     *
     * @throws IOException if unable to communicate with the registry
     * @throws NullPointerException if the VersionedFlow is null, or if its bucket identifier is null, or if the flow to snapshot is null
     * @throws NiFiRegistryException if the flow does not exist
     */
    VersionedFlowSnapshot registerVersionedFlowSnapshot(VersionedFlow flow, VersionedProcessGroup snapshot, String comments, int expectedVersion, NiFiUser user)
        throws IOException, NiFiRegistryException;

    /**
     * Returns the latest (most recent) version of the Flow in the Flow Registry for the given bucket and flow
     *
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     * @return the latest version of the Flow
     *
     * @throws IOException if unable to communicate with the Flow Registry
     * @throws NiFiRegistryException if unable to find the bucket with the given ID or the flow with the given ID
     */
    int getLatestVersion(String bucketId, String flowId, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the contents of the Flow with the given Bucket ID, Flow ID, and version, from the Flow Registry
     *
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     * @param version the version to retrieve
     * @return the contents of the Flow from the Flow Registry
     *
     * @throws IOException if unable to communicate with the Flow Registry
     * @throws NiFiRegistryException if unable to find the contents of the flow due to the bucket or flow not existing,
     *             or the specified version of the flow not existing
     * @throws NullPointerException if any of the arguments is not specified
     * @throws IllegalArgumentException if the given version is less than 1
     */
    VersionedFlowSnapshot getFlowContents(String bucketId, String flowId, int version, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Retrieves the contents of the Flow with the given Bucket ID, Flow ID, and version, from the Flow Registry
     *
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     * @param version the version to retrieve
     * @return the contents of the Flow from the Flow Registry
     *
     * @throws IOException if unable to communicate with the Flow Registry
     * @throws NiFiRegistryException if unable to find the contents of the flow due to the bucket or flow not existing,
     *             or the specified version of the flow not existing
     * @throws NullPointerException if any of the arguments is not specified
     * @throws IllegalArgumentException if the given version is less than 1
     */
    VersionedFlowSnapshot getFlowContents(String bucketId, String flowId, int version) throws IOException, NiFiRegistryException;

    /**
     * Retrieves a VersionedFlow by bucket id and flow id
     *
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     * @return the VersionedFlow for the given bucket and flow ID's
     *
     * @throws IOException if unable to communicate with the Flow Registry
     * @throws NiFiRegistryException if unable to find a flow with the given bucket ID and flow ID
     */
    VersionedFlow getVersionedFlow(String bucketId, String flowId, NiFiUser user) throws IOException, NiFiRegistryException;

    /**
     * Retrieves a VersionedFlow by bucket id and flow id
     *
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     * @return the VersionedFlow for the given bucket and flow ID's
     *
     * @throws IOException if unable to communicate with the Flow Registry
     * @throws NiFiRegistryException if unable to find a flow with the given bucket ID and flow ID
     */
    // TODO: Do we still need this?
    VersionedFlow getVersionedFlow(String bucketId, String flowId) throws IOException, NiFiRegistryException;
}
