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
package org.apache.nifi.integration;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.ExternalControllerServiceReference;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedProcessGroup;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MockSingleFlowRegistryClient implements FlowRegistryClient {
    private final MockFlowRegistry flowRegistry = new MockFlowRegistry();

    @Override
    public FlowRegistry getFlowRegistry(final String registryId) {
        return flowRegistry;
    }

    @Override
    public Set<String> getRegistryIdentifiers() {
        return Collections.singleton("unit-test-registry-client-id");
    }

    @Override
    public void addFlowRegistry(final FlowRegistry registry) {

    }

    @Override
    public FlowRegistry addFlowRegistry(final String registryId, final String registryName, final String registryUrl, final String description) {
        return null;
    }

    @Override
    public FlowRegistry removeFlowRegistry(final String registryId) {
        return null;
    }

    public void addFlow(final String bucketId, final String flowId, final int version, final VersionedFlowSnapshot snapshot) {
        flowRegistry.addFlow(bucketId, flowId, version, snapshot);
    }


    private static class FlowCoordinates {
        private final String bucketId;
        private final String flowId;
        private final int version;

        public FlowCoordinates(final String bucketId, final String flowId, final int version) {
            this.bucketId = bucketId;
            this.flowId = flowId;
            this.version = version;
        }

        public String getBucketId() {
            return bucketId;
        }

        public String getFlowId() {
            return flowId;
        }

        public int getVersion() {
            return version;
        }
    }


    public static class MockFlowRegistry implements FlowRegistry {
        private final Map<FlowCoordinates, VersionedFlowSnapshot> snapshots = new ConcurrentHashMap<>();

        public void addFlow(final String bucketId, final String flowId, final int version, final VersionedFlowSnapshot snapshot) {
            final FlowCoordinates coordinates = new FlowCoordinates(bucketId, flowId, version);
            snapshots.put(coordinates, snapshot);
        }


        @Override
        public String getIdentifier() {
            return "int-test-flow-registry";
        }

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public void setDescription(final String description) {

        }

        @Override
        public String getURL() {
            return "http://localhost:18080/integration-test";
        }

        @Override
        public void setURL(final String url) {

        }

        @Override
        public String getName() {
            return "Integration Test Registry";
        }

        @Override
        public void setName(final String name) {

        }

        @Override
        public Set<Bucket> getBuckets(final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public Bucket getBucket(final String bucketId, final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public Set<VersionedFlow> getFlows(final String bucketId, final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public Set<VersionedFlowSnapshotMetadata> getFlowVersions(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public VersionedFlow registerVersionedFlow(final VersionedFlow flow, final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public VersionedFlow deleteVersionedFlow(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public VersionedFlowSnapshot registerVersionedFlowSnapshot(final VersionedFlow flow, final VersionedProcessGroup snapshot,
                                                                   final Map<String, ExternalControllerServiceReference> externalControllerServices,
                                                                   final Map<String, VersionedParameterContext> parameterContexts, final String comments,
                                                                   final int expectedVersion, final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public int getLatestVersion(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
            return 0;
        }

        @Override
        public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows, final NiFiUser user)
                        throws IOException, NiFiRegistryException {
            return getFlowContents(bucketId, flowId, version, fetchRemoteFlows);
        }

        @Override
        public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows) throws IOException, NiFiRegistryException {
            final FlowCoordinates coordinates = new FlowCoordinates(bucketId, flowId, version);
            return snapshots.get(coordinates);
        }

        @Override
        public VersionedFlow getVersionedFlow(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
            return null;
        }

        @Override
        public VersionedFlow getVersionedFlow(final String bucketId, final String flowId) throws IOException, NiFiRegistryException {
            return null;
        }
    }
}
