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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryFlowRegistry extends AbstractFlowRegistryClient implements FlowRegistryClient {
    private static final String USER_SPECIFIC_ACTIONS_NOT_SUPPORTED = "User-specific actions are not implemented with this Registry";
    private final AtomicInteger flowIdGenerator = new AtomicInteger(1);
    private static final String DEFAULT_BUCKET_ID = "stateless-bucket-1";

    private final Map<FlowCoordinates, List<RegisteredFlowSnapshot>> flowSnapshots = new ConcurrentHashMap<>();

    @Override
    public Set<FlowRegistryBucket> getBuckets(FlowRegistryClientConfigurationContext context) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public FlowRegistryBucket getBucket(FlowRegistryClientConfigurationContext context, String bucketId) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public RegisteredFlow registerFlow(FlowRegistryClientConfigurationContext context, RegisteredFlow flow) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public RegisteredFlow deregisterFlow(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public RegisteredFlow getFlow(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) {
        final FlowCoordinates flowCoordinates = new FlowCoordinates(bucketId, flowId);
        final List<RegisteredFlowSnapshot> snapshots = flowSnapshots.get(flowCoordinates);

        final SimpleRegisteredFlow versionedFlow = new SimpleRegisteredFlow();
        versionedFlow.setBucketIdentifier(bucketId);
        versionedFlow.setBucketName(bucketId);
        versionedFlow.setDescription("Stateless Flow");
        versionedFlow.setIdentifier(flowId);
        versionedFlow.setName(flowId);
        versionedFlow.setVersionCount(snapshots.size());
        return versionedFlow;
    }

    @Override
    public Set<RegisteredFlow> getFlows(FlowRegistryClientConfigurationContext context, String bucketId) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId, final int version) throws FlowRegistryException {
        if (context.getNiFiUserIdentity().isPresent()) {
            throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
        }

        final FlowCoordinates flowCoordinates = new FlowCoordinates(bucketId, flowId);
        final List<RegisteredFlowSnapshot> snapshots = flowSnapshots.get(flowCoordinates);

        final RegisteredFlowSnapshot registeredFlowSnapshot = snapshots.stream()
                .filter(snapshot -> snapshot.getSnapshotMetadata().getVersion() == version)
                .findAny()
                .orElseThrow(() -> new FlowRegistryException("Could not find flow: bucketId=" + bucketId + ", flowId=" + flowId + ", version=" + version));

        return registeredFlowSnapshot;
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(FlowRegistryClientConfigurationContext context, RegisteredFlowSnapshot flowSnapshot) {
        final RegisteredFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        final String bucketId;
        final String flowId;
        final int version;
        if (metadata == null) {
            bucketId = DEFAULT_BUCKET_ID;
            flowId = "flow-" + flowIdGenerator.getAndIncrement();
            version = 1;
        } else {
            bucketId = metadata.getBucketIdentifier();
            flowId = metadata.getFlowIdentifier();
            version = metadata.getVersion();
        }

        final FlowCoordinates coordinates = new FlowCoordinates(bucketId, flowId);

        final List<RegisteredFlowSnapshot> snapshots = flowSnapshots.computeIfAbsent(coordinates, key -> Collections.synchronizedList(new ArrayList<>()));
        final Optional<RegisteredFlowSnapshot> optionalSnapshot = snapshots.stream()
                .filter(snapshot -> snapshot.getSnapshotMetadata().getVersion() == version)
                .findAny();

        if (optionalSnapshot.isPresent()) {
            throw new IllegalStateException("Versioned Flow Snapshot already exists for bucketId=" + bucketId + ", flowId=" + flowId + ", version=" + version);
        }

        snapshots.add(flowSnapshot);
        return flowSnapshot;
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public int getLatestVersion(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    private static class FlowCoordinates {
        private final String bucketId;
        private final String flowId;

        public FlowCoordinates(final String bucketId, final String flowId) {
            this.bucketId = bucketId;
            this.flowId = flowId;
        }

        public String getBucketId() {
            return bucketId;
        }

        public String getFlowId() {
            return flowId;
        }
    }
}
