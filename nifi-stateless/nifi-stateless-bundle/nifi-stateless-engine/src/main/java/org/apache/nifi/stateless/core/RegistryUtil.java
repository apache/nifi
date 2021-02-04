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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RegistryUtil {
    private static final Logger logger = LoggerFactory.getLogger(RegistryUtil.class);

    private final String registryUrl;
    private NiFiRegistryClient registryClient;
    private final SSLContext sslContext;

    public RegistryUtil(final String registryUrl, final SSLContext sslContext) {
        this.registryUrl = registryUrl;
        this.sslContext = sslContext;
    }


    public VersionedFlowSnapshot getFlowByID(String bucketID, String flowID) throws IOException, NiFiRegistryException {
        return getFlowByID(bucketID, flowID, -1);
    }

    public VersionedFlowSnapshot getFlowByID(String bucketID, String flowID, int versionID) throws IOException, NiFiRegistryException {
        if (versionID == -1) {
            // TODO: Have to support providing some sort of user
            versionID = getLatestVersion(bucketID, flowID, null);
        }

        logger.debug("Fetching flow Bucket={}, Flow={}, Version={}, FetchRemoteFlows=true", bucketID, flowID, versionID);
        final long start = System.nanoTime();
        final VersionedFlowSnapshot snapshot = getFlowContents(bucketID, flowID, versionID, true, null);
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.info("Successfully fetched flow from registry in {} millis", millis);

        return snapshot;
    }

    private int getLatestVersion(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        return (int) getFlowClient(user).get(bucketId, flowId).getVersionCount();
    }

    private FlowClient getFlowClient(final NiFiUser user) {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final FlowClient flowClient = identity == null ? registryClient.getFlowClient() : registryClient.getFlowClient(identity);
        return flowClient;
    }

    private FlowSnapshotClient getFlowSnapshotClient(final NiFiUser user) {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final FlowSnapshotClient snapshotClient = identity == null ? registryClient.getFlowSnapshotClient() : registryClient.getFlowSnapshotClient(identity);
        return snapshotClient;
    }

    private synchronized NiFiRegistryClient getRegistryClient() {
        if (registryClient != null) {
            return registryClient;
        }

        final NiFiRegistryClientConfig config = new NiFiRegistryClientConfig.Builder()
            .connectTimeout(30000)
            .readTimeout(30000)
            .sslContext(sslContext)
            .baseUrl(registryUrl)
            .build();

        registryClient = new JerseyNiFiRegistryClient.Builder()
            .config(config)
            .build();

        return registryClient;
    }

    private String getIdentity(final NiFiUser user) {
        return (user == null || user.isAnonymous()) ? null : user.getIdentity();
    }

    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows, final NiFiUser user)
        throws IOException, NiFiRegistryException {

        final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(user);
        final VersionedFlowSnapshot flowSnapshot = snapshotClient.get(bucketId, flowId, version);

        if (fetchRemoteFlows) {
            final VersionedProcessGroup contents = flowSnapshot.getFlowContents();
            for (final VersionedProcessGroup child : contents.getProcessGroups()) {
                populateVersionedContentsRecursively(child, user);
            }
        }

        return flowSnapshot;
    }


    private void populateVersionedContentsRecursively(final VersionedProcessGroup group, final NiFiUser user) throws NiFiRegistryException, IOException {
        if (group == null) {
            return;
        }

        final VersionedFlowCoordinates coordinates = group.getVersionedFlowCoordinates();
        if (coordinates != null) {
            final String registryUrl = coordinates.getRegistryUrl();
            final String bucketId = coordinates.getBucketId();
            final String flowId = coordinates.getFlowId();
            final int version = coordinates.getVersion();

            final RegistryUtil subFlowUtil = new RegistryUtil(registryUrl, sslContext);
            final VersionedFlowSnapshot snapshot = subFlowUtil.getFlowByID(bucketId, flowId, version);
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
            group.setFlowFileConcurrency(contents.getFlowFileConcurrency());
            group.setFlowFileOutboundPolicy(contents.getFlowFileOutboundPolicy());
            group.setDefaultFlowFileExpiration(contents.getDefaultFlowFileExpiration());
            group.setDefaultBackPressureObjectThreshold(contents.getDefaultBackPressureObjectThreshold());
            group.setDefaultBackPressureDataSizeThreshold(contents.getDefaultBackPressureDataSizeThreshold());
            coordinates.setLatest(snapshot.isLatest());
        }

        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            populateVersionedContentsRecursively(child, user);
        }
    }
}
