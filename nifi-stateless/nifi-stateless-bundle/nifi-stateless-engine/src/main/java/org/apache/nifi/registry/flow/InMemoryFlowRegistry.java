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
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedExternalFlowMetadata;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryFlowRegistry implements FlowRegistry {
    private static final String USER_SPECIFIC_ACTIONS_NOT_SUPPORTED = "User-specific actions are not implemented with this Registry";
    private final AtomicInteger flowIdGenerator = new AtomicInteger(1);
    private static final String DEFAULT_BUCKET_ID = "stateless-bucket-1";

    private volatile String description;
    private volatile String name;
    private volatile String url;

    private final Map<FlowCoordinates, List<VersionedExternalFlow>> flowSnapshots = new ConcurrentHashMap<>();

    @Override
    public String getIdentifier() {
        return "in-memory-flow-registry";
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(final String description) {
        this.description = description;
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public void setURL(final String url) {
        this.url = url;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public Set<Bucket> getBuckets(final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public Bucket getBucket(final String bucketId, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public Set<VersionedFlow> getFlows(final String bucketId, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public Set<VersionedFlowSnapshotMetadata> getFlowVersions(final String bucketId, final String flowId, final NiFiUser user)  {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public VersionedFlow registerVersionedFlow(final VersionedFlow flow, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public VersionedFlow deleteVersionedFlow(final String bucketId, final String flowId, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public VersionedFlowSnapshot registerVersionedFlowSnapshot(final VersionedFlow flow, final VersionedProcessGroup snapshot,
                                                               final Map<String, ExternalControllerServiceReference> externalControllerServices,
                                                               final Map<String, VersionedParameterContext> parameterContexts, final String comments,
                                                               final int expectedVersion, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public int getLatestVersion(final String bucketId, final String flowId, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public VersionedFlow getVersionedFlow(final String bucketId, final String flowId, final NiFiUser user) {
        throw new UnsupportedOperationException(USER_SPECIFIC_ACTIONS_NOT_SUPPORTED);
    }

    @Override
    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows) throws NiFiRegistryException {
        final FlowCoordinates flowCoordinates = new FlowCoordinates(bucketId, flowId);
        final List<VersionedExternalFlow> snapshots = flowSnapshots.get(flowCoordinates);

        final VersionedExternalFlow versionedExternalFlow = snapshots.stream()
            .filter(snapshot -> snapshot.getMetadata().getVersion() == version)
            .findAny()
            .orElseThrow(() -> new NiFiRegistryException("Could not find flow: bucketId=" + bucketId + ", flowId=" + flowId + ", version=" + version));

        final VersionedFlowSnapshot versionedFlowSnapshot = convertToVersionedFlowSnapshot(versionedExternalFlow);
        return versionedFlowSnapshot;
    }

    private VersionedFlowSnapshot convertToVersionedFlowSnapshot(final VersionedExternalFlow externalFlow) {
        final VersionedExternalFlowMetadata externalFlowMetadata = externalFlow.getMetadata();

        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(externalFlowMetadata.getBucketIdentifier());
        snapshotMetadata.setVersion(externalFlowMetadata.getVersion());
        snapshotMetadata.setFlowIdentifier(externalFlowMetadata.getFlowIdentifier());

        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setName(externalFlowMetadata.getFlowName());
        versionedFlow.setIdentifier(externalFlowMetadata.getFlowIdentifier());
        versionedFlow.setBucketIdentifier(externalFlowMetadata.getBucketIdentifier());

        final VersionedFlowSnapshot flowSnapshot = new VersionedFlowSnapshot();
        flowSnapshot.setExternalControllerServices(externalFlow.getExternalControllerServices());
        flowSnapshot.setFlowContents(externalFlow.getFlowContents());
        flowSnapshot.setParameterContexts(externalFlow.getParameterContexts());
        flowSnapshot.setSnapshotMetadata(snapshotMetadata);
        flowSnapshot.setFlow(versionedFlow);

        return flowSnapshot;
    }

    @Override
    public VersionedFlow getVersionedFlow(final String bucketId, final String flowId) {
        final FlowCoordinates flowCoordinates = new FlowCoordinates(bucketId, flowId);
        final List<VersionedExternalFlow> snapshots = flowSnapshots.get(flowCoordinates);

        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setBucketIdentifier(bucketId);
        versionedFlow.setBucketName(bucketId);
        versionedFlow.setDescription("Stateless Flow");
        versionedFlow.setIdentifier(flowId);
        versionedFlow.setName(flowId);
        versionedFlow.setVersionCount(snapshots.size());
        return versionedFlow;
    }


    public synchronized void addFlowSnapshot(final VersionedExternalFlow versionedExternalFlow) {
        final VersionedExternalFlowMetadata metadata = versionedExternalFlow.getMetadata();
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

        final List<VersionedExternalFlow> snapshots = flowSnapshots.computeIfAbsent(coordinates, key -> Collections.synchronizedList(new ArrayList<>()));
        final Optional<VersionedExternalFlow> optionalSnapshot = snapshots.stream()
            .filter(snapshot -> snapshot.getMetadata().getVersion() == version)
            .findAny();

        if (optionalSnapshot.isPresent()) {
            throw new IllegalStateException("Versioned Flow Snapshot already exists for bucketId=" + bucketId + ", flowId=" + flowId + ", version=" + version);
        }

        snapshots.add(versionedExternalFlow);
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
