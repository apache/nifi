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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;

public class RestBasedFlowRegistry implements FlowRegistry {

    private final FlowRegistryClient flowRegistryClient;
    private final String identifier;
    private final SSLContext sslContext;
    private volatile String description;
    private volatile String url;
    private volatile String name;

    private NiFiRegistryClient registryClient;

    public RestBasedFlowRegistry(final FlowRegistryClient flowRegistryClient, final String identifier, final String url, final SSLContext sslContext, final String name) {
        this.flowRegistryClient = flowRegistryClient;
        this.identifier = identifier;
        this.url = url;
        this.name = name;
        this.sslContext = sslContext;
    }

    private synchronized NiFiRegistryClient getRegistryClient() {
        if (registryClient != null) {
            return registryClient;
        }

        final NiFiRegistryClientConfig config = new NiFiRegistryClientConfig.Builder()
            .connectTimeout(30000)
            .readTimeout(30000)
            .sslContext(sslContext)
            .baseUrl(url)
            .build();

        registryClient = new JerseyNiFiRegistryClient.Builder()
            .config(config)
            .build();

        return registryClient;
    }

    private synchronized void invalidateClient() {
        this.registryClient = null;
    }

    @Override
    public String getIdentifier() {
        return identifier;
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
    public synchronized void setURL(final String url) {
        this.url = url;
        invalidateClient();
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
    public Set<Bucket> getBuckets(final NiFiUser user) throws IOException, NiFiRegistryException {
        final BucketClient bucketClient = getRegistryClient().getBucketClient(user.isAnonymous() ? null : user.getIdentity());
        return new HashSet<>(bucketClient.getAll());
    }

    @Override
    public Bucket getBucket(final String bucketId) throws IOException, NiFiRegistryException {
        final BucketClient bucketClient = getRegistryClient().getBucketClient();
        return bucketClient.get(bucketId);
    }

    @Override
    public Bucket getBucket(final String bucketId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final BucketClient bucketClient = getRegistryClient().getBucketClient(user.isAnonymous() ? null : user.getIdentity());
        return bucketClient.get(bucketId);
    }


    @Override
    public Set<VersionedFlow> getFlows(final String bucketId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getRegistryClient().getFlowClient(user.isAnonymous() ? null : user.getIdentity());
        return new HashSet<>(flowClient.getByBucket(bucketId));
    }

    @Override
    public Set<VersionedFlowSnapshotMetadata> getFlowVersions(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowSnapshotClient snapshotClient = getRegistryClient().getFlowSnapshotClient(user.isAnonymous() ? null : user.getIdentity());
        return new HashSet<>(snapshotClient.getSnapshotMetadata(bucketId, flowId));
    }

    @Override
    public VersionedFlow registerVersionedFlow(final VersionedFlow flow) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getRegistryClient().getFlowClient();
        return flowClient.create(flow);
    }

    @Override
    public VersionedFlowSnapshot registerVersionedFlowSnapshot(final VersionedFlow flow, final VersionedProcessGroup snapshot, final String comments, final int expectedVersion)
            throws IOException, NiFiRegistryException {

        final FlowSnapshotClient snapshotClient = getRegistryClient().getFlowSnapshotClient();
        final VersionedFlowSnapshot versionedFlowSnapshot = new VersionedFlowSnapshot();
        versionedFlowSnapshot.setFlowContents(snapshot);

        final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
        metadata.setBucketIdentifier(flow.getBucketIdentifier());
        metadata.setFlowIdentifier(flow.getIdentifier());
        metadata.setFlowName(flow.getName());
        metadata.setTimestamp(System.currentTimeMillis());
        metadata.setVersion(expectedVersion);
        metadata.setComments(comments);

        versionedFlowSnapshot.setSnapshotMetadata(metadata);
        return snapshotClient.create(versionedFlowSnapshot);
    }

    @Override
    public int getLatestVersion(final String bucketId, final String flowId) throws IOException, NiFiRegistryException {
        return (int) getRegistryClient().getFlowClient().get(bucketId, flowId).getVersionCount();
    }

    @Override
    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version) throws IOException, NiFiRegistryException {
        final FlowSnapshotClient snapshotClient = getRegistryClient().getFlowSnapshotClient();
        final VersionedFlowSnapshot flowSnapshot = snapshotClient.get(bucketId, flowId, version);

        final VersionedProcessGroup contents = flowSnapshot.getFlowContents();
        for (final VersionedProcessGroup child : contents.getProcessGroups()) {
            populateVersionedContentsRecursively(child);
        }

        return flowSnapshot;
    }

    private void populateVersionedContentsRecursively(final VersionedProcessGroup group) throws NiFiRegistryException, IOException {
        if (group == null) {
            return;
        }

        final VersionedFlowCoordinates coordinates = group.getVersionedFlowCoordinates();
        if (coordinates != null) {
            final String registryUrl = coordinates.getRegistryUrl();
            final String bucketId = coordinates.getBucketId();
            final String flowId = coordinates.getFlowId();
            final int version = coordinates.getVersion();

            final String registryId = flowRegistryClient.getFlowRegistryId(registryUrl);
            if (registryId == null) {
                throw new NiFiRegistryException("Flow contains a reference to another Versioned Flow located at URL " + registryUrl
                    + " but NiFi is not configured to communicate with a Flow Registry at that URL");
            }

            final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
            final VersionedFlowSnapshot snapshot = flowRegistry.getFlowContents(bucketId, flowId, version);
            final VersionedProcessGroup contents = snapshot.getFlowContents();

            group.setComments(contents.getComments());
            group.setConnections(contents.getConnections());
            group.setControllerServices(contents.getControllerServices());
            group.setFunnels(contents.getFunnels());
            group.setInputPorts(contents.getInputPorts());
            group.setLabels(contents.getLabels());
            group.setOutputPorts(contents.getOutputPorts());
            group.setProcessGroups(contents.getProcessGroups());
            group.setProcessors(contents.getProcessors());
            group.setRemoteProcessGroups(contents.getRemoteProcessGroups());
            group.setVariables(contents.getVariables());
        }

        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            populateVersionedContentsRecursively(child);
        }
    }

    @Override
    public VersionedFlow getVersionedFlow(final String bucketId, final String flowId) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getRegistryClient().getFlowClient();
        return flowClient.get(bucketId, flowId);
    }

}
