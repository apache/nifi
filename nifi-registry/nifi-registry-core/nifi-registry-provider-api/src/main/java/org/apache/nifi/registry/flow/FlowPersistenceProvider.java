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

import org.apache.nifi.registry.provider.Provider;

/**
 * A service that can store and retrieve flow contents.
 *
 * The flow contents will be a serialized VersionProcessGroup which came from the flowContents
 * field of a VersionedFlowSnapshot.
 *
 * NOTE: Although this interface is intended to be an extension point, it is not yet considered stable and thus may
 * change across releases until the registry matures.
 */
public interface FlowPersistenceProvider extends Provider {

    /**
     * Persists the serialized content.
     *
     * @param context the context for the content being persisted
     * @param content the serialized flow content to persist
     * @throws FlowPersistenceException if the content could not be persisted
     */
    void saveFlowContent(FlowSnapshotContext context, byte[] content) throws FlowPersistenceException;

    /**
     * Retrieves the serialized content.
     *
     * @param bucketId the bucket id where the flow snapshot is located
     * @param flowId the id of the versioned flow the snapshot belongs to
     * @param version the version of the snapshot
     * @return the bytes for the requested snapshot, or null if not found
     * @throws FlowPersistenceException if the snapshot could not be retrieved due to an error in underlying provider
     */
    byte[] getFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException;

    /**
     * Deletes all content for the versioned flow with the given id in the given bucket.
     *
     * @param bucketId the bucket the versioned flow belongs to
     * @param flowId the id of the versioned flow
     * @throws FlowPersistenceException if the snapshots could not be deleted due to an error in underlying provider
     */
    void deleteAllFlowContent(String bucketId, String flowId) throws FlowPersistenceException;

    /**
     * Deletes the content for the given snapshot.
     *
     * @param bucketId the bucket id where the snapshot is located
     * @param flowId the id of the versioned flow the snapshot belongs to
     * @param version the version of the snapshot
     * @throws FlowPersistenceException if the snapshot could not be deleted due to an error in underlying provider
     */
    void deleteFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException;

}
